package impersonate

import (
	"fmt"
)

type HeaderProfile struct {
	UserAgent    string
	ExtraHeaders map[string]string
}

func BuildClaudeProfile(version, modelName string) HeaderProfile {
	return HeaderProfile{
		UserAgent: fmt.Sprintf("claude-cli/%s (external, cli)", version),
		ExtraHeaders: map[string]string{
			"X-App":                                      "cli",
			"X-Stainless-Runtime":                        "node",
			"X-Stainless-Lang":                           "js",
			"X-Stainless-Package-Version":                "0.94.0",
			"X-Stainless-Runtime-Version":                "v22.12.0",
			"X-Stainless-Arch":                           "arm64",
			"X-Stainless-Os":                             "MacOS",
			"X-Stainless-Retry-Count":                    "0",
			"X-Stainless-Timeout":                        "600",
			"Anthropic-Dangerous-Direct-Browser-Access":  "true",
			"Connection":                                 "keep-alive",
		},
	}
}

func BuildCodexProfile(version string, channelID int) HeaderProfile {
	sessionID := deterministicUUID(fmt.Sprintf("octopus:codex:session:%d", channelID))
	conversationID := randomUUID()

	return HeaderProfile{
		UserAgent: fmt.Sprintf("codex_cli_rs/%s (Mac OS 26.0.1; arm64) Apple_Terminal/464", version),
		ExtraHeaders: map[string]string{
			"Originator":      "codex_cli_rs",
			"Version":         version,
			"Session_id":      sessionID,
			"Conversation_id": conversationID,
			"Connection":      "Keep-Alive",
		},
	}
}

func BuildGeminiProfile(version, modelName string) HeaderProfile {
	return HeaderProfile{
		UserAgent:    fmt.Sprintf("GeminiCLI/%s/%s (linux; x64; terminal)", version, modelName),
		ExtraHeaders: map[string]string{},
	}
}
