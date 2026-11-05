package respond

import (
	"encoding/json"
	"net/http"
)

// JSON sends a JSON response with the given status code and payload.
func JSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// Error sends a JSON error response with the given status code and error message.
func Error(w http.ResponseWriter, status int, message string) {
	JSON(w, status, map[string]string{
		"error": message,
	})
}

// Standard HTTP header names used across handlers and middleware.
const (
	HeaderContentType = "Content-Type"
	HeaderRequestID   = "X-Request-ID"
)
