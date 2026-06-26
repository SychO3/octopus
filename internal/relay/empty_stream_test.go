package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

// Issue #65 Bug 2: a 200 SSE stream that ends without forwarding any payload
// used to return nil from the stream handlers, so attempt() recorded a
// zero-token success, reset the circuit breaker, and pinned stickiness to the
// misbehaving channel. Empty streams must fail the attempt so the relay can
// fail over (nothing was written to the client yet).

func newEmptyStreamTestAttempt(t *testing.T, inType inbound.InboundType, rawFormat transformerModel.APIFormat, outType outbound.OutboundType) (*relayAttempt, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/test", nil)
	internalReq := &transformerModel.InternalLLMRequest{
		Model:        "gpt-4o",
		Stream:       boolPtr(true),
		RawAPIFormat: rawFormat,
	}
	req := &relayRequest{
		c:               c,
		inAdapter:       inbound.Get(inType),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, internalReq.Model, nil, internalReq),
		apiKeyID:        1,
		requestModel:    internalReq.Model,
	}
	return &relayAttempt{
		relayRequest: req,
		outAdapter:   outbound.Get(outType),
	}, recorder
}

func sseTestResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
	}
}

type chunkedReadCloser struct {
	chunks [][]byte
}

func (r *chunkedReadCloser) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := r.chunks[0]
	r.chunks = r.chunks[1:]
	return copy(p, chunk), nil
}

func (r *chunkedReadCloser) Close() error { return nil }

func sseChunkedTestResponse(chunks ...string) *http.Response {
	body := &chunkedReadCloser{chunks: make([][]byte, 0, len(chunks))}
	for _, chunk := range chunks {
		body.chunks = append(body.chunks, []byte(chunk))
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       body,
	}
}

func TestHandleStreamResponseEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	err := ra.handleStreamResponse(context.Background(), sseTestResponse(""))
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty stream, got %v", err)
	}
}

func TestHandleStreamResponseUnconvertibleEventsOnlyFails(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	// Unknown Responses event types produce zero stream events, so nothing is
	// ever forwarded even though the stream carried data lines.
	body := strings.Join([]string{
		`data: {"type":"response.queue_position","position":1}`,
		"",
		`data: {"type":"response.another_unknown_event"}`,
		"",
	}, "\n")
	err := ra.handleStreamResponse(context.Background(), sseTestResponse(body))
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for unconvertible-only stream, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestHandleStreamResponseWithPayloadSucceeds(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIChat, transformerModel.APIFormatOpenAIChatCompletion, outbound.OutboundTypeOpenAIResponse)

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-4o","created_at":1,"output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		"",
	}, "\n")
	if err := ra.handleStreamResponse(context.Background(), sseTestResponse(body)); err != nil {
		t.Fatalf("expected stream with payload to succeed, got %v", err)
	}
	if recorder.Body.Len() == 0 {
		t.Fatalf("expected forwarded payload, got empty body")
	}
}

func TestPassthroughOpenAIResponsesEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeOpenAIResponse, transformerModel.APIFormatOpenAIResponse, outbound.OutboundTypeOpenAIResponse)

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(""), cfg)
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty passthrough stream, got %v", err)
	}
}

func TestPassthroughAnthropicEmptyStreamFails(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	pt := ra.outAdapter.(transformerModel.PassthroughCapable)
	cfg := pt.PassthroughConfig()
	err := ra.handleStreamResponsePassthroughV2(context.Background(), sseTestResponse(""), cfg)
	if !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("expected stream.ErrEmptyUpstreamStream for empty passthrough stream, got %v", err)
	}
}

func TestPassthroughAnthropicEmptyStreamFailsEvenAfterPriorAttemptWrote(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)
	ra.streamPayloadWritten.Store(true)

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(""))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream despite prior written flag, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicConvertedOpenAIEmptyAssistantStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		`data: {"id":"","object":"chat.completion.chunk","created":0,"model":"","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":"stop"}],"usage":{"prompt_tokens":29618,"completion_tokens":1,"total_tokens":29619}}`,
		"",
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for empty assistant stop, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicConvertedOpenAIControlOnlyStreamFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		`data: {"id":"","object":"chat.completion.chunk","created":0,"model":"","choices":[{"index":0,"delta":{"role":"assistant"}}],"usage":{"prompt_tokens":29635,"completion_tokens":1,"total_tokens":29636}}`,
		"",
		`data: {"id":"","object":"chat.completion.chunk","created":0,"model":"","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":29635,"completion_tokens":1,"total_tokens":29636}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for control-only assistant stream, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicConvertedOpenAIJSONEmptyAssistantStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := `{"id":"","choices":[{"index":0,"message":{"role":"assistant"},"finish_reason":"stop"}],"object":"chat.completion","created":0,"model":"","usage":{"prompt_tokens":29625,"completion_tokens":1,"total_tokens":29626,"prompt_tokens_details":null,"completion_tokens_details":null}}`
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), response)
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for empty OpenAI JSON fallback, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicNonStreamOpenAIJSONEmptyAssistantStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := `{"id":"","choices":[{"index":0,"message":{"role":"assistant"},"finish_reason":"stop"}],"object":"chat.completion","created":0,"model":"","usage":{"prompt_tokens":54810,"completion_tokens":1,"total_tokens":54811,"prompt_tokens_details":null,"completion_tokens_details":null}}`
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	err := ra.handleResponsePassthroughAnthropic(context.Background(), response)
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for non-stream empty OpenAI JSON fallback, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicConvertedOpenAISSEFullCompletionEmptyAssistantStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		`data: {"id":"","choices":[{"index":0,"message":{"role":"assistant"},"finish_reason":"stop"}],"object":"chat.completion","created":0,"model":"","usage":{"prompt_tokens":54810,"completion_tokens":1,"total_tokens":54811,"prompt_tokens_details":null,"completion_tokens_details":null}}`,
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for empty OpenAI full-completion SSE, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicNativeEmptyAssistantStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"","type":"message","role":"assistant","model":"","content":[],"usage":{"output_tokens":1}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for native empty assistant stop, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicNativeTerminalChunkAfterPayloadIsForwarded(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	first := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-opus-4-6","usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`,
		"",
		"",
	}, "\n")
	second := strings.Join([]string{
		"event: content_block_stop",
		`data: {"type":"content_block_stop","index":0}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":1}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseChunkedTestResponse(first, second))
	if err != nil {
		t.Fatalf("expected terminal chunk after payload to succeed, got %v", err)
	}
	got := recorder.Body.String()
	if !strings.Contains(got, `"text":"hello"`) {
		t.Fatalf("expected text delta forwarded, got %q", got)
	}
	if !strings.Contains(got, `event: message_stop`) {
		t.Fatalf("expected terminal message_stop forwarded, got %q", got)
	}
}

func TestPassthroughAnthropicNativeDoesNotForwardPartialFrame(t *testing.T) {
	ra, _ := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)
	writer := &notifyStreamWriter{header: http.Header{}}
	ra.streamWriter = writer

	completeFrames := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-opus-4-6","usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"Write","input":{}}}`,
		"",
		"",
	}, "\n")
	partialFrame := "event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp`
	frameRemainder := `\"}"}}` + "\n\n"
	terminalFrames := strings.Join([]string{
		"event: content_block_stop",
		`data: {"type":"content_block_stop","index":0}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":5}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")
	writer.onWrite = func(p []byte) {
		if bytes.Contains(p, []byte(partialFrame)) && !bytes.Contains(p, []byte(partialFrame+frameRemainder)) {
			t.Fatalf("forwarded an incomplete SSE frame in one write: %q", string(p))
		}
	}

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseChunkedTestResponse(completeFrames+partialFrame, frameRemainder+terminalFrames))
	if err != nil {
		t.Fatalf("expected split frame stream to succeed, got %v", err)
	}
	got := writer.buf.String()
	if !strings.Contains(got, completeFrames) {
		t.Fatalf("expected complete initial frames to be forwarded, got %q", got)
	}
	if !strings.Contains(got, partialFrame+frameRemainder) {
		t.Fatalf("expected completed split frame to be forwarded, got %q", got)
	}
	for _, want := range []string{
		`event: content_block_stop`,
		`event: message_stop`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %s to be forwarded, got %q", want, got)
		}
	}
}

func TestPassthroughAnthropicNativeTextFrameSplitAcrossChunks(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	firstFrame := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-opus-4-6","usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
		"",
	}, "\n")
	partialSecond := "event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hel`
	secondRemainder := `lo"}}` + "\n\n" +
		strings.Join([]string{
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
		}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseChunkedTestResponse(firstFrame+partialSecond, secondRemainder))
	if err != nil {
		t.Fatalf("expected split frame stream to succeed, got %v", err)
	}
	got := recorder.Body.String()
	if strings.Contains(got, partialSecond) && !strings.Contains(got, partialSecond+`lo"}}`) {
		t.Fatalf("forwarded an incomplete SSE frame: %q", got)
	}
	if !strings.Contains(got, firstFrame) {
		t.Fatalf("expected complete first frame to be forwarded, got %q", got)
	}
	if !strings.Contains(got, `event: content_block_delta`) || !strings.Contains(got, `event: message_stop`) {
		t.Fatalf("expected completed split frame and terminal frame to be forwarded, got %q", got)
	}
}

func TestPassthroughAnthropicNativeEmptyAssistantUsageDeltaStopFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"","type":"message","role":"assistant","content":[],"model":"","usage":{"input_tokens":54810,"output_tokens":1}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":54810,"output_tokens":1}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for native empty usage delta stop, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}

func TestPassthroughAnthropicNativeRepeatedEmptyAssistantFailsBeforeWrite(t *testing.T) {
	ra, recorder := newEmptyStreamTestAttempt(t, inbound.InboundTypeAnthropic, transformerModel.APIFormatAnthropicMessage, outbound.OutboundTypeAnthropic)

	body := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"msg_empty_1","type":"message","role":"assistant","content":[],"model":"claude-opus-4-6","usage":{"input_tokens":14939,"output_tokens":0}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":null,"stop_sequence":null},"usage":{"input_tokens":14939,"output_tokens":0}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
		"event: message_start",
		`data: {"type":"message_start","message":{"id":"","type":"message","role":"assistant","content":[],"model":"","usage":{"input_tokens":29625,"output_tokens":1}}}`,
		"",
	}, "\n")

	err := ra.handleStreamResponsePassthroughAnthropic(context.Background(), sseTestResponse(body))
	if !errors.Is(err, errEmptyUpstreamStream) {
		t.Fatalf("expected errEmptyUpstreamStream for repeated native empty assistant events, got %v", err)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("expected nothing forwarded to client, got %q", recorder.Body.String())
	}
}
