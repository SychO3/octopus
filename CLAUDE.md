# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 开发命令

### 后端 (Go)
```bash
go run main.go start                # 启动服务 (默认 0.0.0.0:8080)
go run main.go start --config path  # 指定配置文件
go test ./...                       # 运行所有测试
```

### 前端 (Next.js)
```bash
cd web
pnpm install                        # 安装依赖
pnpm dev                            # 开发服务器 (localhost:3000)
NEXT_PUBLIC_API_BASE_URL="http://127.0.0.1:8080" pnpm dev  # 指定后端地址
pnpm build                          # 生产构建 (输出到 web/out/)
pnpm lint                           # ESLint 检查
```

### 完整构建
```bash
cd web && pnpm install && pnpm build && cd ..
mv web/out static/
go run main.go start
```

### 跨平台发布
```bash
./scripts/build.sh build linux x86_64   # 构建指定平台
./scripts/build.sh release              # 构建所有平台
```

### Docker 本地开发部署
```bash
# 首次需要创建 data 目录
mkdir -p data && chmod 777 data

# 构建并启动 (代码变更后重新执行)
docker compose -f docker-compose.dev.yml up -d --build

# 查看日志
docker compose -f docker-compose.dev.yml logs -f

# 停止
docker compose -f docker-compose.dev.yml down
```

初始账号 `admin` / `admin`，服务端口 8080。
构建配置见 `Dockerfile.dev` + `docker-compose.dev.yml`。

**每次修改代码后必须 commit，再构建部署。**

### 生产部署（当前）

架构：源站在加拿大，美国仅做 Nginx 反代 + SSL。

| 角色 | 位置 | 说明 |
|------|------|------|
| 源站 | 加拿大 `142.4.219.49` | Docker 跑 Octopus，宿主机端口 **18081→容器 8080**（本机 8080 被 bepusdt 占用） |
| 反代 | 美国 `154.44.14.43` | Nginx 终结 SSL，回源加拿大 |
| 域名 | `oct.uf.gs` | 唯一正式入口（旧 `ai.515111.xyz` 已下线） |
| 镜像 | `ghcr.io/sycho3/octopus:dev` | **本机源码构建**的本地 tag（默认不推 GHCR） |

**加拿大源站路径**
- 代码仓库：`/root/octopus`（改代码、构建镜像）
- 部署目录：`/root/octopus-app/`
- compose：`/root/octopus-app/docker-compose.yml`（`image: ghcr.io/sycho3/octopus:dev`，宿主机 `18081:8080`）
- 数据：`/root/octopus-app/data/`（`config.json` + SQLite `data.db` + `octopus.db`）
- 生产容器不 bind-mount 源码目录

**本地构建并部署（默认流程，不要 push）**
```bash
# 1) 在源码目录构建镜像（打成 compose 使用的 tag）
cd /root/octopus
docker build -f Dockerfile.dev \
  --build-arg GIT_VERSION=dev \
  --build-arg GIT_AUTHOR=SychO3 \
  --build-arg BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --build-arg COMMIT_ID="$(git rev-parse --short HEAD)" \
  -t ghcr.io/sycho3/octopus:dev \
  .

# 2) 用本地镜像重建容器（不要 docker compose pull）
cd /root/octopus-app
docker compose up -d --force-recreate --no-deps octopus

# 3) 自检
docker compose ps
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18081/
docker compose logs -f
```

**美国反代**
- 配置：`/etc/nginx/conf.d/oct-ufgs.conf`
- 证书：`/etc/nginx/cert/oct.uf.gs/`（acme.sh + Cloudflare DNS）
- 回源：`http://142.4.219.49:18081`
- SSE/流式：关缓冲，长超时（7200s）
- 修改后：`nginx -t && nginx -s reload`

**数据说明**
- 业务库为 SQLite；`relay_logs` 会迅速膨胀，迁移/备份时通常排除日志、保留配置与统计。
- 热导出示例（不停服）：`.backup` → `DELETE FROM relay_logs; VACUUM;`

**部署约定**
- 改代码后：在 `/root/octopus` 本地 `docker build` → `/root/octopus-app` 用本地镜像 `compose up -d --force-recreate`
- **默认不要 `git push`，也不要 `docker compose pull` / 推 GHCR**（除非用户明确要求）
- 不要把生产 data 提交进 git

## 架构概览

Octopus 是一个 **LLM API 聚合与负载均衡服务**。Go 后端 (Gin + GORM) 提供 API 代理和管理接口，Next.js 前端提供管理面板。

**启动流程**: `main.go` → `cmd/start.go` → 初始化 Config → DB → Cache → HTTP Server → Background Tasks

**请求流**: Gin Router → Middleware (Auth/CORS/Logger) → Handler → Op (业务逻辑) → DB/Cache

**API 代理流**: Request → Inbound Transformer (协议转换) → Relay → Balancer (负载均衡/熔断) → 外部 LLM API → Outbound Transformer → Response

## 后端关键模块 (`internal/`)

| 模块 | 职责 |
|------|------|
| `conf/` | Viper 配置管理，env 前缀 `OCTOPUS_`，默认读取 `data/config.json` |
| `db/` | GORM 数据库层，支持 SQLite(默认)/MySQL/PostgreSQL，`db/migrate/` 含迁移 |
| `model/` | 数据模型定义 (Channel, Group, User, APIKey, Setting, Stats 等) |
| `op/` | **业务逻辑层 (Service)**，包含内存缓存管理，Handler 调用此层而非直接操作 DB |
| `server/handlers/` | HTTP 请求处理器，按资源分文件 |
| `server/middleware/` | Auth (JWT + API Key)、CORS、Logger、Static 等中间件 |
| `server/router/` | 自定义路由框架，链式注册: `NewGroupRouter(path).Use(mw).AddRoute(route)` |
| `server/auth/` | JWT 生成/验证，API Key 格式 `sk-octopus-*` |
| `server/resp/` | 统一响应格式 `{code, message, data}` |
| `relay/` | API 代理核心，负载均衡策略 (RoundRobin/Random/Failover/Weighted)，熔断器 |
| `transformer/` | 协议转换适配器，`inbound/` 解析请求，`outbound/` 格式化响应，支持 OpenAI/Anthropic/Gemini |
| `task/` | 后台定时任务 (统计持久化、模型同步、价格更新) |
| `client/` | LLM 提供商 HTTP 客户端封装 |
| `utils/log/` | Zap 结构化日志 |
| `utils/cache/` | 分片缓存 (16 shard, xxhash)，运行时内存缓存 + 关机持久化到 DB |

## 前端关键模式 (`web/src/`)

- **状态管理**: Zustand (本地/持久化状态) + TanStack React Query (服务端数据缓存，30s 自动刷新)
- **UI**: shadcn/ui + Radix UI 原语 + TailwindCSS v4 + Framer Motion 动画
- **路由**: 自定义 SPA 路由 (`route/config.tsx` 定义，`ContentLoader` 动态加载)，**不使用** Next.js 文件路由
- **API 层**: `api/client.ts` 基于 fetch 的 HTTP 客户端，`api/endpoints/` 按功能导出 React Query hooks
- **i18n**: next-intl，翻译文件位于 `public/locale/{en,zh_hans,zh_hant}.json`
- **构建**: SSG 静态导出 (`output: "export"`)，嵌入到 Go 二进制的 `static/` 目录

## 配置

运行时配置 `data/config.json`（首次运行自动生成），所有字段可通过 `OCTOPUS_` 前缀环境变量覆盖:
- `OCTOPUS_SERVER_PORT`, `OCTOPUS_SERVER_HOST`
- `OCTOPUS_DATABASE_TYPE` (sqlite/mysql/postgres), `OCTOPUS_DATABASE_PATH`
- `OCTOPUS_LOG_LEVEL`

数据库运行时设置 (CORS 等) 存储在 `Setting` 模型中，通过 `op/setting.go` 缓存访问。

## 贡献规范

- 每个 PR 只包含一个变更主题（一个功能或一个 BUG 修复）
- AI 辅助代码需完成人工审查后提交
