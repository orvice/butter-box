package app

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestWithBearerAuth(t *testing.T) {
	const token = "secret-token"

	tests := []struct {
		name   string
		config string // token configured on the server
		header string // Authorization header sent by the client
		want   int
	}{
		{"valid token is accepted", token, "Bearer " + token, http.StatusOK},
		{"extra whitespace around the token is trimmed", token, "  Bearer " + token + "  ", http.StatusOK},
		{"missing header is rejected", token, "", http.StatusUnauthorized},
		{"empty header is rejected", token, " ", http.StatusUnauthorized},
		{"non-bearer scheme is rejected", token, "Basic abc", http.StatusUnauthorized},
		{"wrong token is rejected", token, "Bearer wrong", http.StatusUnauthorized},
		// Fail closed: an empty configured token (e.g. a future caller that
		// forgets to wire auth) must reject every request, never pass them
		// through unauthenticated.
		{"empty configured token rejects everything", "", "Bearer anything", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := WithBearerAuth(okHandler(), tt.config)
			req := httptest.NewRequest(http.MethodPost, "/butterbox.pi.v1.PiService/GetAvailableModels", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
			if tt.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("expected WWW-Authenticate challenge on rejection")
			}
		})
	}
}
