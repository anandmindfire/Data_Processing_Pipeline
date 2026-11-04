package models

import "time"

// Job represents the persistent state and results of a pipeline job.
type Job struct {
	ID          string         `json:"id"`
	Status      JobStatus      `json:"status"`
	Config      Config         `json:"config"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Metrics     Metrics        `json:"metrics"`
	ResultsPath string         `json:"results_path,omitempty"`
	Aggregates  map[string]any `json:"aggregates,omitempty"`
}
