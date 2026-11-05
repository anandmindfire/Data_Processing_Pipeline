package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
)

// requestIDKey represents the context key type for storing the request ID.
type requestIDKey struct{}

const maxRequestIDLength = 64

// validRequestID accepts caller-supplied IDs only if they are short and use a safe character set,
// so a client cannot inject arbitrary text into logs or response headers.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLength {
		return false
	}
	for _, c := range id {
		safe := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
		if !safe {
			return false
		}
	}
	return true
}

// RequestID propagates a valid incoming X-Request-ID or generates a new one.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get(respond.HeaderRequestID)
		if !validRequestID(reqID) {
			var b [16]byte
			_, _ = rand.Read(b[:])
			reqID = hex.EncodeToString(b[:])
		}
		w.Header().Set(respond.HeaderRequestID, reqID)
		ctx := context.WithValue(r.Context(), requestIDKey{}, reqID)
		ctx = context.WithValue(ctx, requestMetaKey{}, &requestMeta{})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extracts the request ID from the context.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// requestMeta carries facts discovered by inner middleware (such as the authenticated key) back
// out to outer middleware like the access logger, which only sees the original request.
type requestMeta struct {
	mu    sync.Mutex
	keyID string
}

type requestMetaKey struct{}

func setRequestKeyID(ctx context.Context, id string) {
	if m, ok := ctx.Value(requestMetaKey{}).(*requestMeta); ok {
		m.mu.Lock()
		m.keyID = id
		m.mu.Unlock()
	}
}

// requestKeyID returns the API key ID that authenticated the request, if any.
func requestKeyID(ctx context.Context) string {
	if m, ok := ctx.Value(requestMetaKey{}).(*requestMeta); ok {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.keyID
	}
	return ""
}
