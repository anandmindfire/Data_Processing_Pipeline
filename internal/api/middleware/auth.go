package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// RootKeyID identifies requests authenticated with a bootstrap API_KEY.
const RootKeyID = "root"

// Principal is the authenticated caller of a request.
type Principal struct {
	KeyID string // managed key ID, or RootKeyID for the bootstrap API_KEY
	Name  string
	Admin bool // root keys only: may manage API keys
}

type principalKey struct{}

// PrincipalFrom returns the authenticated caller, or nil when authentication is disabled.
func PrincipalFrom(ctx context.Context) *Principal {
	p, _ := ctx.Value(principalKey{}).(*Principal)
	return p
}

// WithPrincipal returns a context carrying p (useful in tests and internal calls).
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// KeyVerifier resolves a managed key to its principal; it returns an error for unknown,
// revoked, or expired keys.
type KeyVerifier func(ctx context.Context, key string) (*Principal, error)

// AuthOptions configures authentication.
type AuthOptions struct {
	MetricsPublic bool        // allow unauthenticated Prometheus scrapes of /metrics
	Verifier      KeyVerifier // managed keys (nil = only the root keys are accepted)
}

// APIKeyAuth authenticates requests when at least one root key is configured.
//
// Root keys (API_KEY) are compared as SHA-256 digests in constant time, so neither key contents
// nor key length leak through timing, and grant admin rights. Other keys are checked by the
// verifier (managed, expiring keys). Without root keys authentication is disabled, since there
// would be no way to manage keys.
func APIKeyAuth(rootKeys []string, opts AuthOptions) func(http.Handler) http.Handler {
	digests := make([][32]byte, 0, len(rootKeys))
	for _, k := range rootKeys {
		if k != "" {
			digests = append(digests, sha256.Sum256([]byte(k)))
		}
	}
	return func(next http.Handler) http.Handler {
		if len(digests) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublicRoute(r.URL.Path, opts) || r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}
			key := extractAPIKey(r)
			p := authenticate(r.Context(), key, digests, opts.Verifier)
			if p == nil {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", `Bearer realm="pipeline"`)
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized: invalid, expired, or missing API key"})
				return
			}
			setRequestKeyID(r.Context(), p.KeyID)
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
		})
	}
}

func authenticate(ctx context.Context, key string, rootDigests [][32]byte, verify KeyVerifier) *Principal {
	if key == "" {
		return nil
	}
	provided := sha256.Sum256([]byte(key))
	match := 0
	for i := range rootDigests {
		match |= subtle.ConstantTimeCompare(provided[:], rootDigests[i][:])
	}
	if match == 1 {
		return &Principal{KeyID: RootKeyID, Name: "root", Admin: true}
	}
	if verify == nil {
		return nil
	}
	p, err := verify(ctx, key)
	if err != nil {
		return nil
	}
	return p
}

// RequireAdmin allows only principals authenticated with a root key. With authentication
// disabled (no API_KEY), admin endpoints are refused because no caller can be trusted to manage
// credentials.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := PrincipalFrom(r.Context())
		if p == nil || !p.Admin {
			msg := "forbidden: API key management requires the admin key (API_KEY)"
			if p == nil {
				msg = "forbidden: API key management is disabled because API_KEY is not configured"
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isPublicRoute reports whether a request path bypasses API key authentication.
func isPublicRoute(path string, opts AuthOptions) bool {
	switch path {
	case "/healthz", "/readyz":
		return true
	case "/metrics":
		return opts.MetricsPublic
	default:
		return path == "/docs" || strings.HasPrefix(path, "/docs/") || strings.HasPrefix(path, "/swagger")
	}
}

// extractAPIKey pulls an API key token from X-API-Key or Bearer Authorization headers.
func extractAPIKey(r *http.Request) string {
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}
