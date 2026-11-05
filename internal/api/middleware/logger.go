package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// statusRecorder captures the status code and response size.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Logger logs one structured line per request. The query string is omitted because it may carry
// identifiers; credentials are never logged since only the path is recorded.
func Logger(ips *IPResolver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			level := slog.LevelInfo
			switch {
			case rec.status >= 500:
				level = slog.LevelError
			case rec.status >= 400:
				level = slog.LevelWarn
			case r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics":
				level = slog.LevelDebug // keep probe noise out of info logs
			}
			slog.Log(r.Context(), level, "http request",
				"request_id", GetRequestID(r.Context()),
				"key_id", requestKeyID(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"client_ip", ips.ClientIP(r),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}
