package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// statusLoggingResponseWriter wraps http.ResponseWriter to capture the returned HTTP status code.
type statusLoggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the HTTP status code before delegating to the wrapped response writer.
func (w *statusLoggingResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Logger returns an HTTP middleware that logs incoming requests with duration and status code.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusLoggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"remote_addr", r.RemoteAddr,
			"duration_ms", duration.Milliseconds(),
		)
	})
}
