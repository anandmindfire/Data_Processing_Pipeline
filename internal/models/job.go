package models

import "time"

// JobStatus describes the current lifecycle state of a pipeline job.
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
)

// Job represents the persistent state and results of a pipeline job.
type Job struct {
	ID          string         `json:"id"`
	Status      JobStatus      `json:"status"`
	Config      Config         `json:"config"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Metrics     Metrics        `json:"metrics"`
	ResultsPath string         `json:"results_path,omitempty"`
	Artifacts   []string       `json:"artifacts,omitempty"` // every file written for the job (exports, sidecars, DLQ)
	Aggregates  map[string]any `json:"aggregates,omitempty"`
	Error       string         `json:"error,omitempty"` // terminal failure or cancellation reason

}

// IsTerminal reports whether the status is a final state.
func (s JobStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}
