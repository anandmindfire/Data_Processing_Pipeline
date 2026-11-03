package models

import "time"

// Source defines a data ingestion source.
type Source struct {
	Type    string            `json:"type"`              // "csv", "json", "http", "api"
	Name    string            `json:"name"`              // Unique identifier for the source
	Path    string            `json:"path,omitempty"`    // Local file path
	URL     string            `json:"url,omitempty"`     // External HTTP API endpoint
	Headers map[string]string `json:"headers,omitempty"` // Request headers for HTTP sources
	Format  string            `json:"format,omitempty"`  // Data format ("csv", "json")
}

// Rule defines validation and transformation constraints for a specific record field.
type Rule struct {
	Field     string   `json:"field"`               // Field name
	Required  bool     `json:"required,omitempty"`  // Must be non-empty if true
	Type      string   `json:"type,omitempty"`      // "string", "number"
	Min       *float64 `json:"min,omitempty"`       // Minimum numeric threshold
	Max       *float64 `json:"max,omitempty"`       // Maximum numeric threshold
	Normalize string   `json:"normalize,omitempty"` // "trim", "lower", "upper"
	Multiply  float64  `json:"multiply,omitempty"`  // Scaling factor for numeric values
}

// Aggregation defines an aggregation to compute over processed records.
type Aggregation struct {
	GroupBy   string `json:"group_by,omitempty"` // Optional field to group records by
	Field     string `json:"field"`              // Target field to compute aggregation on
	Operation string `json:"operation"`          // "count", "sum", "avg", "average"
	Name      string `json:"name,omitempty"`     // Custom alias name for output
}

// ExportTarget defines where processed data should be persisted.
type ExportTarget struct {
	Type string `json:"type"`           // "json", "csv"
	Path string `json:"path,omitempty"` // File destination path
}

// Config specifies the complete specification for a pipeline execution job.
type Config struct {
	Sources           []Source       `json:"sources"`
	Rules             []Rule         `json:"rules,omitempty"`
	Aggregations      []Aggregation  `json:"aggregations,omitempty"`
	Exports           []ExportTarget `json:"exports"`
	ValidationWorkers int            `json:"validation_workers,omitempty"`
	TransformWorkers  int            `json:"transform_workers,omitempty"`
	BufferSize        int            `json:"buffer_size,omitempty"`
	TimeoutSeconds    int            `json:"timeout_seconds,omitempty"`
	MaxRecords        int            `json:"max_records,omitempty"`
}

// JobStatus describes the current lifecycle state of a pipeline job.
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
)

// Record is an untyped dictionary representing a single data entity.
type Record map[string]any

// RecordError encapsulates an error encountered by a specific record in any pipeline stage.
type RecordError struct {
	Source  string    `json:"source"`
	Index   int       `json:"index"`
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
	Record  Record    `json:"record,omitempty"`
	At      time.Time `json:"at"`
}

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

