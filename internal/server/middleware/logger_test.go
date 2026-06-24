package middleware

import "testing"

func TestIsLongLivedStreamPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/api/v1/log/stream", want: true},
		{path: "/api/v1/log/stream-token", want: false},
		{path: "/v1/messages", want: false},
	}

	for _, tt := range tests {
		if got := isLongLivedStreamPath(tt.path); got != tt.want {
			t.Fatalf("isLongLivedStreamPath(%q) = %t, want %t", tt.path, got, tt.want)
		}
	}
}
