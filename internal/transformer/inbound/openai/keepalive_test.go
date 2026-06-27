package openai

import (
	"context"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestChatInboundStreamKeepAlive(t *testing.T) {
	i := &ChatInbound{}
	payload := i.StreamKeepAlive()
	s := string(payload)
	if !strings.HasPrefix(s, "data: ") || !strings.HasSuffix(s, "\n\n") {
		t.Fatalf("keepalive must be a complete SSE data frame, got %q", s)
	}
	if !strings.Contains(s, `"chat.completion.chunk"`) || !strings.Contains(s, `"choices":[]`) {
		t.Fatalf("keepalive must be an empty-choices chunk, got %q", s)
	}
}

func TestResponseInboundStreamKeepAliveBeforeCreatedIsNil(t *testing.T) {
	i := &ResponseInbound{}
	if payload := i.StreamKeepAlive(); payload != nil {
		t.Fatalf("expected nil keepalive before response.created, got %q", string(payload))
	}
}

func TestResponseInboundStreamKeepAliveAfterCreated(t *testing.T) {
	i := &ResponseInbound{}
	ctx := context.Background()

	// Drive a MessageStart so the stream emits response.created/in_progress and
	// advances the sequence counter.
	if _, err := i.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_ka", Model: "gpt-5.5", Role: "assistant"},
	}); err != nil {
		t.Fatalf("TransformStreamEvents failed: %v", err)
	}

	payload := i.StreamKeepAlive()
	if payload == nil {
		t.Fatal("expected non-nil keepalive after response.created")
	}
	s := string(payload)
	if !strings.Contains(s, "response.in_progress") {
		t.Fatalf("keepalive must be a response.in_progress event, got %q", s)
	}

	// The keepalive must continue the sequence (no gap/duplicate): the created +
	// in_progress events used sequence 0 and 1, so the keepalive must be 2.
	if !strings.Contains(s, `"sequence_number":2`) {
		t.Fatalf("keepalive must continue sequence_number (expected 2), got %q", s)
	}
}
