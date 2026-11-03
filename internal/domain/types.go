package domain

import "time"

type Source struct {
	Type    string            `json:"type"`
	Name    string            `json:"name"`
	Path    string            `json:"path,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Format  string            `json:"format,omitempty"`
}
type Rule struct {
	Field     string   `json:"field"`
	Required  bool     `json:"required,omitempty"`
	Type      string   `json:"type,omitempty"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Normalize string   `json:"normalize,omitempty"`
	Multiply  float64  `json:"multiply,omitempty"`
}
type Aggregation struct {
	GroupBy   string `json:"group_by,omitempty"`
	Field     string `json:"field"`
	Operation string `json:"operation"`
	Name      string `json:"name,omitempty"`
}
type ExportTarget struct {
	Type string `json:"type"`
	Path string `json:"path,omitempty"`
}
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
type JobStatus string

const (
	Pending   JobStatus = "PENDING"
	Running   JobStatus = "RUNNING"
	Completed JobStatus = "COMPLETED"
	Failed    JobStatus = "FAILED"
	Cancelled JobStatus = "CANCELLED"
)

type Record map[string]any
type RecordError struct {
	Source  string    `json:"source"`
	Index   int       `json:"index"`
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
	Record  Record    `json:"record,omitempty"`
	At      time.Time `json:"at"`
}
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
