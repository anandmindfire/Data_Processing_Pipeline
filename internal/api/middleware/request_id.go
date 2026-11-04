package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/pkg/constants"
)

// requestIDKey represents the context key type for storing the request ID.
type requestIDKey struct{}

// RequestID attaches a unique identifier to incoming requests if not already present.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get(constants.HeaderRequestID)
		if reqID == "" {
			var b [16]byte
			_, _ = rand.Read(b[:])
			reqID = hex.EncodeToString(b[:])
		}

		w.Header().Set(constants.HeaderRequestID, reqID)
		ctx := context.WithValue(r.Context(), requestIDKey{}, reqID)
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
