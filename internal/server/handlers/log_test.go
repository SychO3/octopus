package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/op"
	"github.com/gin-gonic/gin"
)

func TestStreamLogFlushesInitialSSEFrameWhenIdle(t *testing.T) {
	gin.SetMode(gin.TestMode)

	token, err := op.RelayLogStreamTokenCreate()
	if err != nil {
		t.Fatalf("RelayLogStreamTokenCreate failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/stream?token="+token, nil).WithContext(ctx)
	recorder := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(recorder)
	c.Request = req

	streamLog(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("expected text/event-stream content type, got %q", got)
	}
	if body := recorder.Body.String(); !strings.Contains(body, ": connected\n\n") {
		t.Fatalf("expected initial SSE comment frame, got %q", body)
	}
}
