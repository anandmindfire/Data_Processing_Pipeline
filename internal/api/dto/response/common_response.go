package response

// CommonResponse describes a standard JSON envelope.
type CommonResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
}

// StatusResponse represents an acknowledgment response (e.g. for cancellation or deletion).
type StatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
