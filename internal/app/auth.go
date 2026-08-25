package app

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// WithBearerAuth protects next with HTTP Bearer token auth and always enforces
// it: requests without a valid token are answered 401 and never forwarded.
//
// It deliberately fails closed — an empty configured token rejects every
// request instead of passing them all through. Exposing the MCP endpoint or
// the Pi API unauthenticated would hand anyone who can reach the box shell
// access and pi model credentials, so LoadConfig refuses to start without
// MCP_AUTH_TOKEN; this handler is the independent second line of defense in
// case a future caller forgets to wire auth.
func WithBearerAuth(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="butter-box"`)
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}

		provided := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="butter-box"`)
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
