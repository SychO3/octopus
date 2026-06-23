package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/bestruirui/octopus/internal/impersonate"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/gin-gonic/gin"
)

func init() {
	router.NewGroupRouter("/api/v1/impersonate").
		Use(middleware.Auth()).
		AddRoute(router.NewRoute("/versions", http.MethodGet).Handle(getImpersonateVersions)).
		AddRoute(router.NewRoute("/refresh", http.MethodPost).Handle(refreshImpersonateVersions))
}

func getImpersonateVersions(c *gin.Context) {
	claudeCustom, _ := op.SettingGetString(model.SettingKeyCLIVersionsClaude)
	codexCustom, _ := op.SettingGetString(model.SettingKeyCLIVersionsCodex)
	geminiCustom, _ := op.SettingGetString(model.SettingKeyCLIVersionsGemini)
	claudeLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsClaudeLatest)
	codexLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsCodexLatest)
	geminiLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsGeminiLatest)
	updatedAt, _ := op.SettingGetString(model.SettingKeyCLIVersionsUpdatedAt)

	resp.Success(c, gin.H{
		"claude":     claudeLatest,
		"codex":      codexLatest,
		"gemini":     geminiLatest,
		"claude_custom": claudeCustom,
		"codex_custom":  codexCustom,
		"gemini_custom": geminiCustom,
		"updated_at": updatedAt,
	})
}

func refreshImpersonateVersions(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Minute)
	defer cancel()

	if err := impersonate.RefreshAllVersions(ctx); err != nil {
		resp.Error(c, http.StatusInternalServerError, "refresh failed: "+err.Error())
		return
	}

	claudeLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsClaudeLatest)
	codexLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsCodexLatest)
	geminiLatest, _ := op.SettingGetString(model.SettingKeyCLIVersionsGeminiLatest)
	updatedAt, _ := op.SettingGetString(model.SettingKeyCLIVersionsUpdatedAt)

	resp.Success(c, gin.H{
		"claude":     claudeLatest,
		"codex":      codexLatest,
		"gemini":     geminiLatest,
		"updated_at": updatedAt,
	})
}
