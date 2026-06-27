package helper

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// EndpointProbeVerdict 单个端点格式的探测判定
type EndpointProbeVerdict string

const (
	ProbeSupported    EndpointProbeVerdict = "supported"    // 真 200 / 429：该格式可用
	ProbeNoRoute      EndpointProbeVerdict = "no_route"     // 404/405：上游无此端点
	ProbeInconclusive EndpointProbeVerdict = "inconclusive" // 401/403/402/5xx/超时：非类型信号
)

// EndpointProbeOutcome 一个渠道的探测结论
type EndpointProbeOutcome struct {
	ChannelID    int                                           `json:"channel_id"`
	ChannelName  string                                        `json:"channel_name"`
	CurrentType  outbound.OutboundType                         `json:"current_type"`
	DetectedType outbound.OutboundType                         `json:"detected_type"`
	Conclusive   bool                                          `json:"conclusive"` // 能否确定 type（确定才应锁定）
	Changed      bool                                          `json:"changed"`    // DetectedType != CurrentType
	PerFormat    map[outbound.OutboundType]EndpointProbeVerdict `json:"per_format"`
	Reason       string                                        `json:"reason"`
}

// candidateProbeTypes 参与自动探测的 chat 族端点格式（按推荐优先级排列）。
// Embedding/Volcengine 稀有且请求形态不同，不参与自动探测。
var candidateProbeTypes = []outbound.OutboundType{
	outbound.OutboundTypeOpenAIChat,
	outbound.OutboundTypeOpenAIResponse,
	outbound.OutboundTypeAnthropic,
	outbound.OutboundTypeGemini,
}

const probeCandidateTimeout = 15 * time.Second

// 较长的、像真实对话的提示词：很多渠道会对 "hi"/"ping" 这类极小请求做防探测处理
// （不返回 / 返回异常），用正常对话可让"真 200 + 真实响应体"的判定可靠。
const probeUserPrompt = "Hello! I'm testing the connectivity of this API endpoint. " +
	"Could you please reply with a short friendly sentence to confirm you received this message? " +
	"For example, tell me one interesting fact about the number seven. Thanks a lot!"

// ProbeChannelEndpoint 对渠道实测探测真实端点格式，并给出是否应改正 type 的结论。
//
// 策略（最小化 key 消耗、不 churn 正常渠道）：
//  1. 先探当前 type；若真的可用 → 保持当前、结论确定（可锁定）。
//  2. 当前 type 为确定性 404 → 再探其余格式，挑一个可用的切换、结论确定（可锁定）。
//  3. 当前 type 为瞬时失败（5xx/超时/鉴权）→ 结论不确定，不改不锁，下次重试。
func ProbeChannelEndpoint(ctx context.Context, channel model.Channel) EndpointProbeOutcome {
	outcome := EndpointProbeOutcome{
		ChannelID:    channel.ID,
		ChannelName:  channel.Name,
		CurrentType:  channel.Type,
		DetectedType: channel.Type,
		PerFormat:    map[outbound.OutboundType]EndpointProbeVerdict{},
	}

	modelName := firstChannelModel(channel)
	if modelName == "" {
		outcome.Reason = "no model name on channel"
		return outcome
	}
	usedKey := channel.GetChannelKey()
	if strings.TrimSpace(usedKey.ChannelKey) == "" {
		outcome.Reason = "no usable key"
		return outcome
	}

	// 1. 先探当前 type
	curVerdict := probeOneFormat(ctx, channel, usedKey.ChannelKey, modelName, channel.Type)
	outcome.PerFormat[channel.Type] = curVerdict
	if curVerdict == ProbeSupported {
		outcome.Conclusive = true
		outcome.Reason = "current type works"
		return outcome
	}
	if curVerdict == ProbeInconclusive {
		// 瞬时/鉴权问题，无法判定类型，保持现状等下次
		outcome.Reason = "current type inconclusive (transient/auth), keep and retry later"
		return outcome
	}

	// 2. 当前 type 为 no_route：探其余候选，挑一个可用的切换
	for _, t := range candidateProbeTypes {
		if t == channel.Type {
			continue
		}
		v := probeOneFormat(ctx, channel, usedKey.ChannelKey, modelName, t)
		outcome.PerFormat[t] = v
		if v == ProbeSupported {
			outcome.DetectedType = t
			outcome.Changed = true
			outcome.Conclusive = true
			outcome.Reason = "current type has no route; switched to a working format"
			return outcome
		}
	}
	outcome.Reason = "current type has no route but no working format found"
	return outcome
}

func firstChannelModel(channel model.Channel) string {
	for _, name := range strings.Split(channel.Model, ",") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// probeOneFormat 用指定端点格式向渠道真实上游发一次探测请求并判定。
func probeOneFormat(ctx context.Context, channel model.Channel, key, modelName string, probeType outbound.OutboundType) EndpointProbeVerdict {
	adapter := outbound.Get(probeType)
	if adapter == nil {
		return ProbeInconclusive
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeCandidateTimeout)
	defer cancel()

	request := buildEndpointProbeRequest(probeType, modelName)
	// 用对应格式的真实出站适配器构造逐字节一致的请求
	probeChannel := channel
	probeChannel.Type = probeType
	httpReq, err := adapter.TransformRequest(probeCtx, request, probeChannel.GetBaseUrl(), key)
	if err != nil {
		return ProbeInconclusive
	}
	applyProbeHeaders(httpReq, channel.CustomHeader)
	if err := ApplyParamOverride(httpReq, channel.ParamOverride); err != nil {
		return ProbeInconclusive
	}

	httpClient, err := ChannelHTTPClientWithContext(probeCtx, &probeChannel)
	if err != nil {
		return ProbeInconclusive
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return ProbeInconclusive
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
	return classifyProbeResponse(resp.StatusCode, body)
}

// classifyProbeResponse 复用实测验证过的判定逻辑：
//   - 真 200（响应体是真实补全，非错误体）/ 429 → supported
//   - 404/405 → no_route
//   - 其余（401/403/402/5xx/超时等）→ inconclusive（非类型信号）
func classifyProbeResponse(status int, body []byte) EndpointProbeVerdict {
	switch {
	case status == 200:
		if bodyLooksLikeRealCompletion(body) {
			return ProbeSupported
		}
		// 部分网关对无效路径也返回 200 + 错误体，属假成功
		return ProbeNoRoute
	case status == 429:
		return ProbeSupported
	case status == 404 || status == 405:
		return ProbeNoRoute
	default:
		return ProbeInconclusive
	}
}

func bodyLooksLikeRealCompletion(body []byte) bool {
	low := strings.ToLower(string(body))
	if low == "" {
		return false
	}
	// 明确的错误体特征：无效路由
	if strings.Contains(low, "invalid url") || strings.Contains(low, "no route") ||
		strings.Contains(low, "cannot post") || strings.Contains(low, "404 page not found") {
		return false
	}
	// 真实补全的正向结构标志，覆盖各协议且多出现在响应体前部：
	//   OpenAI chat: {"id":"chatcmpl-...","object":"chat.completion","choices":[...]}
	//   OpenAI responses: {"id":"resp_...","object":"response","status":"completed",...}
	//     （responses 会先回显超长 instructions，output 数组可能在读取窗口之外，
	//      故须用前部的 object/id 标志判定，不能只依赖 output）
	//   Anthropic: {"id":"msg_...","content":[...]}  Gemini: {"candidates":[...]}
	//   Embedding: {"object":"list","data":[...]}
	positives := []string{
		`"object":"response"`, `"object": "response"`,
		`"object":"chat.completion"`, `"object": "chat.completion"`,
		`"object":"list"`, `"object": "list"`,
		`"id":"resp_`, `"id": "resp_`, `"id":"chatcmpl`, `"id": "chatcmpl`,
		`"choices"`, `"candidates"`, `"content"`, `"output"`,
	}
	for _, marker := range positives {
		if strings.Contains(low, marker) {
			return true
		}
	}
	// 兜底：成功响应常带 "error":null，先剔除再判断是否还残留 error 包裹。
	stripped := strings.NewReplacer(`"error":null`, "", `"error": null`, "").Replace(low)
	return !strings.Contains(stripped, `"error"`)
}

func applyProbeHeaders(request *http.Request, headers []model.CustomHeader) {
	if request == nil {
		return
	}
	// 防止 Go 默认 User-Agent 泄露/被 WAF 拦截
	if request.Header.Get("User-Agent") == "" {
		request.Header.Set("User-Agent", probeUserAgent)
	}
	for _, header := range headers {
		key := strings.TrimSpace(header.HeaderKey)
		if key == "" {
			continue
		}
		request.Header.Set(key, header.HeaderValue)
	}
}

const probeUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"

func buildEndpointProbeRequest(probeType outbound.OutboundType, modelName string) *transformerModel.InternalLLMRequest {
	stream := false
	prompt := probeUserPrompt
	maxTok := int64(24)
	msgs := []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &prompt}}}

	switch probeType {
	case outbound.OutboundTypeOpenAIResponse:
		return &transformerModel.InternalLLMRequest{
			Model: modelName, RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
			Messages: msgs, Stream: &stream, MaxCompletionTokens: &maxTok,
		}
	case outbound.OutboundTypeAnthropic:
		return &transformerModel.InternalLLMRequest{
			Model: modelName, RawAPIFormat: transformerModel.APIFormatAnthropicMessage,
			Messages: msgs, Stream: &stream, MaxTokens: &maxTok,
		}
	case outbound.OutboundTypeGemini:
		return &transformerModel.InternalLLMRequest{
			Model: modelName, RawAPIFormat: transformerModel.APIFormatGeminiContents,
			Messages: msgs, Stream: &stream, MaxTokens: &maxTok,
		}
	default:
		return &transformerModel.InternalLLMRequest{
			Model: modelName, RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
			Messages: msgs, Stream: &stream, MaxTokens: &maxTok,
		}
	}
}

