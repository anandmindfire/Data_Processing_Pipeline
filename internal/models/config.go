package models

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

