package response

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// ErrorResponse describes an HTTP error returned by the API.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Details any    `json:"details,omitempty"`
}

// ErrorsListResponse wraps a collection of record errors.
type ErrorsListResponse struct {
	JobID  string               `json:"job_id"`
	Total  int                  `json:"total"`
	Errors []models.RecordError `json:"errors"`
}
