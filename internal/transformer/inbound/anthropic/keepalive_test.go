package anthropic

import (
	"strings"
	"testing"
)

func TestMessagesInboundStreamKeepAlive(t *testing.T) {
	i := &MessagesInbound{}
	s := string(i.StreamKeepAlive())
	if !strings.Contains(s, "event:ping") {
		t.Fatalf("keepalive must be an Anthropic ping event, got %q", s)
	}
	if !strings.Contains(s, `"type":"ping"`) {
		t.Fatalf("keepalive ping must carry type=ping, got %q", s)
	}
	if !strings.HasSuffix(s, "\n\n") {
		t.Fatalf("keepalive must be a complete SSE frame, got %q", s)
	}
}
