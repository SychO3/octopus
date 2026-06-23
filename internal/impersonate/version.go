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

var versionSettingKeys = map[ProviderType]model.SettingKey{
	ProviderClaude: model.SettingKeyCLIVersionsClaude,
	ProviderCodex:  model.SettingKeyCLIVersionsCodex,
	ProviderGemini: model.SettingKeyCLIVersionsGemini,
}

func GetEffectiveVersion(provider ProviderType) string {
	if key, ok := versionSettingKeys[provider]; ok {
		if v, err := op.SettingGetString(key); err == nil && v != "" {
			return v
		}
	}

	if v, ok := fallbackVersions[provider]; ok {
		return v
	}
	return "unknown"
}
