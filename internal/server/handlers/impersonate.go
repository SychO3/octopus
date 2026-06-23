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
	claudeVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsClaude)
	codexVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsCodex)
	geminiVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsGemini)
	updatedAt, _ := op.SettingGetString(model.SettingKeyCLIVersionsUpdatedAt)

	resp.Success(c, gin.H{
		"claude":     claudeVer,
		"codex":      codexVer,
		"gemini":     geminiVer,
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

	claudeVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsClaude)
	codexVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsCodex)
	geminiVer, _ := op.SettingGetString(model.SettingKeyCLIVersionsGemini)
	updatedAt, _ := op.SettingGetString(model.SettingKeyCLIVersionsUpdatedAt)

	resp.Success(c, gin.H{
		"claude":     claudeVer,
		"codex":      codexVer,
		"gemini":     geminiVer,
		"updated_at": updatedAt,
	})
}
