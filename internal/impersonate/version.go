package impersonate

import (
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

type ProviderType string

const (
	ProviderClaude ProviderType = "claude"
	ProviderCodex  ProviderType = "codex"
	ProviderGemini ProviderType = "gemini"
)

var fallbackVersions = map[ProviderType]string{
	ProviderClaude: "2.1.183",
	ProviderCodex:  "0.141.0",
	ProviderGemini: "0.31.0",
}

// versionSettingKeys 用户自定义版本 key
var versionSettingKeys = map[ProviderType]model.SettingKey{
	ProviderClaude: model.SettingKeyCLIVersionsClaude,
	ProviderCodex:  model.SettingKeyCLIVersionsCodex,
	ProviderGemini: model.SettingKeyCLIVersionsGemini,
}

// versionLatestKeys 自动拉取版本 key
var versionLatestKeys = map[ProviderType]model.SettingKey{
	ProviderClaude: model.SettingKeyCLIVersionsClaudeLatest,
	ProviderCodex:  model.SettingKeyCLIVersionsCodexLatest,
	ProviderGemini: model.SettingKeyCLIVersionsGeminiLatest,
}

// GetEffectiveVersion 优先级：用户自定义 > 自动拉取 > 硬编码 fallback
func GetEffectiveVersion(provider ProviderType) string {
	// 1. 用户自定义
	if key, ok := versionSettingKeys[provider]; ok {
		if v, err := op.SettingGetString(key); err == nil && v != "" {
			return v
		}
	}
	// 2. 自动拉取的最新版本
	if key, ok := versionLatestKeys[provider]; ok {
		if v, err := op.SettingGetString(key); err == nil && v != "" {
			return v
		}
	}
	// 3. Fallback
	if v, ok := fallbackVersions[provider]; ok {
		return v
	}
	return "unknown"
}
