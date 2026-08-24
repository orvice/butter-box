package app

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
)

func TestRewriteSameOriginHeader(t *testing.T) {
	target := &url.URL{Scheme: "http", Host: "127.0.0.1:30141"}

	tests := []struct {
		name       string
		host       string
		origin     string
		wantOrigin string
	}{
		{
			name:       "same-origin request is rewritten to target",
			host:       "box.example.com:8080",
			origin:     "https://box.example.com:8080",
			wantOrigin: "http://127.0.0.1:30141",
		},
		{
			name:       "cross-site origin passes through unchanged",
			host:       "box.example.com:8080",
			origin:     "https://evil.example.com",
			wantOrigin: "https://evil.example.com",
		},
		{
			name:       "null origin passes through unchanged",
			host:       "box.example.com:8080",
			origin:     "null",
			wantOrigin: "null",
		},
		{
			name:       "no origin header stays absent",
			host:       "box.example.com:8080",
			origin:     "",
			wantOrigin: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := httptest.NewRequest(http.MethodPost, "http://"+tt.host+"/api/cwd/validate", nil)
			in.Host = tt.host
			if tt.origin != "" {
				in.Header.Set("Origin", tt.origin)
			}

			pr := &httputil.ProxyRequest{In: in, Out: in.Clone(in.Context())}
			rewriteSameOriginHeader(pr, target)

			if got := pr.Out.Header.Get("Origin"); got != tt.wantOrigin {
				t.Errorf("Origin = %q, want %q", got, tt.wantOrigin)
			}
		})
	}
}
