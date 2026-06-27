package stream

import (
	"bytes"
	"context"
	"sync"
)

// ResponsesKeepAliveCapturer wraps a StreamSource and taps the passing bytes to
// capture the first complete OpenAI Responses "response.in_progress" (preferred)
// or "response.created" SSE event. That event is a stateless snapshot carrying no
// incremental content, so replaying it verbatim during an upstream silence resets
// stall timers on clients that ignore SSE comments — without inventing a new
// sequence_number (which would risk gaps/duplicates on the strict Responses wire
// format).
//
// Capture happens in the source read goroutine; KeepAlive() is read from the
// processor's main loop goroutine, hence the mutex.
type ResponsesKeepAliveCapturer struct {
	inner StreamSource

	mu       sync.Mutex
	pending  bytes.Buffer // accumulates bytes until the first event is captured
	captured []byte       // best replayable event so far (created fallback / in_progress)
	done     bool         // stop accumulating once finalized or buffer overflows
}

// NewResponsesKeepAliveCapturer wraps src to capture a replayable keep-alive event.
func NewResponsesKeepAliveCapturer(src StreamSource) *ResponsesKeepAliveCapturer {
	return &ResponsesKeepAliveCapturer{inner: src}
}

const keepAliveCaptureMaxBuffer = 64 * 1024

func (c *ResponsesKeepAliveCapturer) ReadEvent(ctx context.Context) ([]byte, error) {
	data, err := c.inner.ReadEvent(ctx)
	if len(data) > 0 {
		c.observe(data)
	}
	return data, err
}

func (c *ResponsesKeepAliveCapturer) Close() error {
	return c.inner.Close()
}

// KeepAlive returns the captured event bytes, or nil if none captured yet.
func (c *ResponsesKeepAliveCapturer) KeepAlive() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.captured
}

// observe scans newly-read raw bytes for the first replayable event. SSE events
// are delimited by a blank line ("\n\n"); RawSource chunks are not event-aligned,
// so bytes are buffered across reads until a full event boundary is seen.
func (c *ResponsesKeepAliveCapturer) observe(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return
	}

	c.pending.Write(data)

	buf := c.pending.Bytes()
	for {
		idx := bytes.Index(buf, []byte("\n\n"))
		if idx < 0 {
			break
		}
		block := buf[:idx]
		buf = buf[idx+2:]

		switch responsesEventType(block) {
		case "response.in_progress":
			// Preferred keep-alive: "still working" snapshot. Capture and stop.
			c.captured = appendEventTerminator(block)
			c.done = true
			c.pending.Reset()
			return
		case "response.created":
			// Usable fallback, but keep scanning in case in_progress follows.
			if c.captured == nil {
				c.captured = appendEventTerminator(block)
			}
		case "":
			// Non-typed frame (e.g. SSE comment); ignore.
		default:
			// A content-bearing event arrived; stop with whatever snapshot we have
			// so we never replay incremental deltas as a keep-alive.
			c.done = true
			c.pending.Reset()
			return
		}
	}

	// Keep only the unparsed tail; cap memory in case upstream never emits "\n\n".
	if len(buf) > keepAliveCaptureMaxBuffer {
		c.done = true
		c.pending.Reset()
		return
	}
	c.pending.Reset()
	c.pending.Write(buf)
}

// appendEventTerminator returns the SSE event block with its trailing "\n\n".
func appendEventTerminator(block []byte) []byte {
	out := make([]byte, 0, len(block)+2)
	out = append(out, block...)
	out = append(out, '\n', '\n')
	return out
}

// responsesEventType extracts the Responses event type from an SSE event block,
// reading the explicit "event:" line when present, else the JSON "type" field on
// the "data:" line.
func responsesEventType(block []byte) string {
	for _, line := range bytes.Split(block, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if rest, ok := bytes.CutPrefix(line, []byte("event:")); ok {
			return string(bytes.TrimSpace(rest))
		}
		if rest, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			return jsonTypeField(bytes.TrimSpace(rest))
		}
	}
	return ""
}

// jsonTypeField pulls the "type" string field from a JSON object without full
// unmarshaling.
func jsonTypeField(data []byte) string {
	idx := bytes.Index(data, []byte(`"type"`))
	if idx < 0 {
		return ""
	}
	rest := data[idx+len(`"type"`):]
	if i := bytes.IndexByte(rest, ':'); i >= 0 {
		rest = bytes.TrimSpace(rest[i+1:])
		if len(rest) > 0 && rest[0] == '"' {
			rest = rest[1:]
			if end := bytes.IndexByte(rest, '"'); end >= 0 {
				return string(rest[:end])
			}
		}
	}
	return ""
}
