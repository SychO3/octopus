package relay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
)

func TestStreamContentDetectionDoesNotConsumeAnthropicLogResponse(t *testing.T) {
	ctx := context.Background()
	inAdapter := inbound.Get(inbound.InboundTypeAnthropic)
	eventAdapter := inAdapter.(transformerModel.InboundStreamEventTransformer)
	internalRequest := &transformerModel.InternalLLMRequest{
		Model:        "claude-opus-4-8",
		RawAPIFormat: transformerModel.APIFormatAnthropicMessage,
	}
	metrics := NewRelayMetrics(1, internalRequest.Model, nil, internalRequest)
	ra := &relayAttempt{relayRequest: &relayRequest{
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		requestModel:    internalRequest.Model,
	}}

	if _, err := eventAdapter.TransformStreamEvents(ctx, []transformerModel.StreamEvent{
		{Kind: transformerModel.StreamEventKindMessageStart, ID: "resp_1", Model: internalRequest.Model, Role: "assistant"},
		{Kind: transformerModel.StreamEventKindTextDelta, ID: "resp_1", Model: internalRequest.Model, Delta: &transformerModel.StreamDelta{Text: "我是 Claude。"}},
	}); err != nil {
		t.Fatalf("transform content events: %v", err)
	}

	if !ra.streamHasSubstantiveContent() {
		t.Fatal("text delta must be recognized as substantive content")
	}

	if _, err := eventAdapter.TransformStreamEvents(ctx, []transformerModel.StreamEvent{
		{Kind: transformerModel.StreamEventKindMessageStop, ID: "resp_1", Model: internalRequest.Model, StopReason: transformerModel.FinishReasonStop},
		{Kind: transformerModel.StreamEventKindUsageDelta, ID: "resp_1", Model: internalRequest.Model, Usage: &transformerModel.Usage{PromptTokens: 10, CompletionTokens: 4}},
	}); err != nil {
		t.Fatalf("transform terminal events: %v", err)
	}

	ra.collectResponse()
	if len(metrics.ClientFacingResponse) == 0 {
		t.Fatal("expected client-facing response to be collected")
	}

	var response struct {
		Content []struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(metrics.ClientFacingResponse, &response); err != nil {
		t.Fatalf("decode client-facing response: %v", err)
	}
	if len(response.Content) != 1 || response.Content[0].Text == nil || *response.Content[0].Text != "我是 Claude。" {
		t.Fatalf("stream validation must not consume response content, got %s", metrics.ClientFacingResponse)
	}
}
