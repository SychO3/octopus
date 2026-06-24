package stream

import (
	"context"
)

// WSUpstreamReader abstracts WebSocket upstream reader interface.
// This avoids circular dependency with internal/relay package.
type WSUpstreamReader interface {
	ReadEvent(ctx context.Context) ([]byte, error)
	Close() error
	CloseWithError()
	StatusCode() int
}

// WSSource wraps a WebSocket upstream reader.
type WSSource struct {
	reader WSUpstreamReader
}

// NewWSSource creates a source from a WebSocket reader.
func NewWSSource(reader WSUpstreamReader) *WSSource {
	return &WSSource{reader: reader}
}

// ReadEvent reads the next WebSocket event.
func (s *WSSource) ReadEvent(ctx context.Context) ([]byte, error) {
	return s.reader.ReadEvent(ctx)
}

// Close is a no-op: the WebSocket connection lifecycle (pool return vs removal)
// is owned by the relay caller, which calls reader.Close()/CloseWithError()
// based on success/failure after the processor returns. Closing here would race
// with the caller's close and double-return the pooled connection.
func (s *WSSource) Close() error {
	return nil
}
