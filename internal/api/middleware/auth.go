package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// APIKeyAuth enforces API key verification if configuredKey is non-empty.
func APIKeyAuth(configuredKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if configuredKey == "" {
				next.ServeHTTP(w, r)
				return
			}

			path := r.URL.Path
			if isPublicRoute(path) {
				next.ServeHTTP(w, r)
				return
			}

			providedKey := extractAPIKey(r)
			if subtle.ConstantTimeCompare([]byte(providedKey), []byte(configuredKey)) != 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized: invalid or missing API key"}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// isPublicRoute reports whether a request path bypasses API key authentication.
func isPublicRoute(path string) bool {
	return path == "/healthz" ||
		path == "/metrics" ||
		strings.HasPrefix(path, "/docs") ||
		strings.HasPrefix(path, "/swagger")
}

// extractAPIKey pulls an API key token from X-API-Key or Bearer Authorization headers.
func extractAPIKey(r *http.Request) string {
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return ""
}
