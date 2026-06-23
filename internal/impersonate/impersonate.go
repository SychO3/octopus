package impersonate

import (
	"net/http"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func ApplyClientHeaders(req *http.Request, channel *model.Channel, modelName string) {
	provider := detectProvider(channel)
	version := GetEffectiveVersion(provider)

	var profile HeaderProfile
	switch provider {
	case ProviderClaude:
		profile = BuildClaudeProfile(version, modelName)
	case ProviderCodex:
		profile = BuildCodexProfile(version, channel.ID)
	case ProviderGemini:
		profile = BuildGeminiProfile(version, modelName)
	default:
		return
	}

	req.Header.Set("User-Agent", profile.UserAgent)
	for k, v := range profile.ExtraHeaders {
		req.Header.Set(k, v)
	}
}

// ApplyClientHeadersToMap 与 ApplyClientHeaders 相同逻辑，但操作 http.Header map（用于 WebSocket 等非标准路径）
func ApplyClientHeadersToMap(headers http.Header, channel *model.Channel, modelName string) {
	provider := detectProvider(channel)
	version := GetEffectiveVersion(provider)

	var profile HeaderProfile
	switch provider {
	case ProviderClaude:
		profile = BuildClaudeProfile(version, modelName)
	case ProviderCodex:
		profile = BuildCodexProfile(version, channel.ID)
	case ProviderGemini:
		profile = BuildGeminiProfile(version, modelName)
	default:
		return
	}

	headers.Set("User-Agent", profile.UserAgent)
	for k, v := range profile.ExtraHeaders {
		headers.Set(k, v)
	}
}

func detectProvider(channel *model.Channel) ProviderType {
	channelType := outbound.OutboundType(channel.Type)
	switch channelType {
	case outbound.OutboundTypeAnthropic:
		return ProviderClaude
	case outbound.OutboundTypeOpenAIChat, outbound.OutboundTypeOpenAIResponse:
		return ProviderCodex
	case outbound.OutboundTypeGemini:
		return ProviderGemini
	default:
		return ProviderCodex
	}
}
