package helper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestClassifyProbeResponse(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   EndpointProbeVerdict
	}{
		{"real-200-chat", 200, `{"choices":[{"message":{"content":"hi"}}]}`, ProbeSupported},
		{"real-200-anthropic", 200, `{"content":[{"type":"text","text":"hi"}]}`, ProbeSupported},
		{"real-200-gemini", 200, `{"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}`, ProbeSupported},
		// Responses 成功体：带 "error":null，且 output 在超长 instructions 回显之后
		{"real-200-responses-errnull", 200, `{"id":"resp_abc","object":"response","status":"completed","error":null,"instructions":"You are GPT-5.1 running in the Codex CLI ` + strings.Repeat("x", 9000) + `"}`, ProbeSupported},
		{"real-200-chatcmpl-id", 200, `{"id":"chatcmpl-xyz","object":"chat.completion","error":null}`, ProbeSupported},
		{"fake-200-invalid-url", 200, `{"error":{"message":"Invalid URL (POST /v1/v1beta/...)"}}`, ProbeNoRoute},
		{"fake-200-error-body", 200, `{"error":{"message":"something"}}`, ProbeNoRoute},
		{"ratelimit-429", 429, `{"error":"rate limited"}`, ProbeSupported},
		{"no-route-404", 404, `404 page not found`, ProbeNoRoute},
		{"no-route-405", 405, ``, ProbeNoRoute},
		{"auth-401", 401, `{"error":"invalid key"}`, ProbeInconclusive},
		{"balance-402", 402, `{"error":"Insufficient credit"}`, ProbeInconclusive},
		{"forbidden-403", 403, `<html>cloudflare</html>`, ProbeInconclusive},
		{"upstream-500", 500, `{"error":"upstream error"}`, ProbeInconclusive},
		{"upstream-503", 503, ``, ProbeInconclusive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyProbeResponse(tc.status, []byte(tc.body))
			if got != tc.want {
				t.Fatalf("classify(%d, %q) = %q, want %q", tc.status, tc.body, got, tc.want)
			}
		})
	}
}

// TestProbeChannelEndpointSwitchesOn404 复现 id=102：当前 Response 端点 404，
// 但 /chat/completions 返回真实补全 → 应切换到 Chat 且结论确定。
func TestProbeChannelEndpointSwitchesOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/chat/completions"):
			w.WriteHeader(200)
			w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Seven is prime."}}]}`))
		case strings.HasSuffix(r.URL.Path, "/responses"):
			w.WriteHeader(404)
			w.Write([]byte(`404 page not found`))
		default:
			w.WriteHeader(404)
			w.Write([]byte(`404 page not found`))
		}
	}))
	defer srv.Close()

	channel := model.Channel{
		ID:       102,
		Name:     "test-mislabeled-response",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Model:    "gpt-5.4-mini",
		BaseUrls: []model.BaseUrl{{URL: srv.URL + "/v1"}},
		Keys:     []model.ChannelKey{{ID: 1, Enabled: true, ChannelKey: "sk-test"}},
	}

	outcome := ProbeChannelEndpoint(context.Background(), channel)
	if !outcome.Conclusive {
		t.Fatalf("expected conclusive, got inconclusive: %s", outcome.Reason)
	}
	if !outcome.Changed {
		t.Fatalf("expected changed, got unchanged")
	}
	if outcome.DetectedType != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("detected type = %d, want Chat(%d)", outcome.DetectedType, outbound.OutboundTypeOpenAIChat)
	}
}

// TestProbeChannelEndpointKeepsWorkingType 当前 type 实测可用 → 保持、确定、不改。
func TestProbeChannelEndpointKeepsWorkingType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer srv.Close()

	channel := model.Channel{
		ID: 1, Name: "test-chat", Type: outbound.OutboundTypeOpenAIChat, Model: "gpt-4",
		BaseUrls: []model.BaseUrl{{URL: srv.URL + "/v1"}},
		Keys:     []model.ChannelKey{{ID: 1, Enabled: true, ChannelKey: "sk-test"}},
	}
	outcome := ProbeChannelEndpoint(context.Background(), channel)
	if !outcome.Conclusive || outcome.Changed {
		t.Fatalf("expected conclusive & unchanged, got conclusive=%v changed=%v (%s)", outcome.Conclusive, outcome.Changed, outcome.Reason)
	}
}

// TestProbeChannelEndpointInconclusiveOnTransient 当前 type 瞬时 5xx → 不确定、不改。
func TestProbeChannelEndpointInconclusiveOnTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte(`{"error":"upstream temporarily unavailable"}`))
	}))
	defer srv.Close()

	channel := model.Channel{
		ID: 1, Name: "test-transient", Type: outbound.OutboundTypeAnthropic, Model: "claude-opus-4-6",
		BaseUrls: []model.BaseUrl{{URL: srv.URL + "/v1"}},
		Keys:     []model.ChannelKey{{ID: 1, Enabled: true, ChannelKey: "sk-test"}},
	}
	outcome := ProbeChannelEndpoint(context.Background(), channel)
	if outcome.Conclusive {
		t.Fatalf("expected inconclusive on transient 5xx, got conclusive (changed=%v)", outcome.Changed)
	}
}
