package stream

import (
	"context"
	"testing"
)

func drain(t *testing.T, c *ResponsesKeepAliveCapturer) {
	t.Helper()
	ctx := context.Background()
	for {
		_, err := c.ReadEvent(ctx)
		if err != nil {
			return
		}
	}
}

func TestKeepAliveCapturer_CapturesInProgress(t *testing.T) {
	// Chunks deliberately split an event across two reads to exercise cross-read
	// buffering (RawSource is not event-aligned).
	src := newMockStreamSource([][]byte{
		[]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0}\n\n"),
		[]byte("event: response.in_progress\ndata: {\"type\":\"response.in_pro"),
		[]byte("gress\",\"sequence_number\":1}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"}\n\n"),
	})
	c := NewResponsesKeepAliveCapturer(src)
	drain(t, c)

	ka := string(c.KeepAlive())
	if ka == "" {
		t.Fatal("expected a captured keepalive event")
	}
	if !contains(ka, "response.in_progress") {
		t.Fatalf("expected in_progress to be preferred, got %q", ka)
	}
	if !hasSuffix(ka, "\n\n") {
		t.Fatalf("captured event must be a complete SSE frame, got %q", ka)
	}
}

func TestKeepAliveCapturer_FallsBackToCreated(t *testing.T) {
	src := newMockStreamSource([][]byte{
		[]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0}\n\n"),
		[]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"}\n\n"),
	})
	c := NewResponsesKeepAliveCapturer(src)
	drain(t, c)

	ka := string(c.KeepAlive())
	if !contains(ka, "response.created") {
		t.Fatalf("expected response.created capture, got %q", ka)
	}
}

func TestKeepAliveCapturer_StopsOnContentBeforeSnapshot(t *testing.T) {
	// A content event arriving before any created/in_progress snapshot must not be
	// replayed as a keepalive.
	src := newMockStreamSource([][]byte{
		[]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"}\n\n"),
		[]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":5}\n\n"),
	})
	c := NewResponsesKeepAliveCapturer(src)
	drain(t, c)

	if ka := c.KeepAlive(); ka != nil {
		t.Fatalf("expected no keepalive when content precedes snapshot, got %q", string(ka))
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}
