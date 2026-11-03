package models

import "time"

// Metrics captures real-time observability telemetry for a pipeline execution.
type Metrics struct {
	Discovered       int64                    `json:"discovered"`
	Processed        int64                    `json:"processed"`
	Valid            int64                    `json:"valid"`
	Rejected         int64                    `json:"rejected"`
	Exported         int64                    `json:"exported"`
	Errors           int64                    `json:"errors"`
	StartedAt        time.Time                `json:"started_at,omitempty"`
	EndedAt          *time.Time               `json:"ended_at,omitempty"`
	RecordsPerSecond float64                  `json:"records_per_second"`
	StageLatency     map[string]time.Duration `json:"stage_latency_ns"`
}

// Progress provides structured real-time progress for client polling.
type Progress struct {
	JobID            string                   `json:"job_id"`
	Status           JobStatus                `json:"status"`
	Percent          float64                  `json:"percent_complete"`
	Processed        int64                    `json:"processed"`
	Discovered       int64                    `json:"discovered"`
	Pending          int64                    `json:"pending"`
	Valid            int64                    `json:"valid"`
	Rejected         int64                    `json:"rejected"`
	ErrorCount       int64                    `json:"error_count"`
	StartedAt        time.Time                `json:"started_at,omitempty"`
	EndedAt          *time.Time               `json:"ended_at,omitempty"`
	RecordsPerSecond float64                  `json:"records_per_second"`
	StageLatency     map[string]time.Duration `json:"stage_latency_ns"`
}

