package middleware

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Recovery catches panics from downstream handlers, logs the stack trace, and returns HTTP 500.
// http.ErrAbortHandler is re-raised so net/http can abort the connection as intended.
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rec)
			}
			slog.Error("panic recovered",
				"request_id", GetRequestID(r.Context()),
				"error", rec,
				"stack", string(debug.Stack()),
				"path", r.URL.Path,
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":      "internal server error",
				"request_id": GetRequestID(r.Context()),
			})
		}()
		next.ServeHTTP(w, r)
	})
}
