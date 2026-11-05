package models

// Source defines a data ingestion source.
type Source struct {
	Type    string            `json:"type"`              // "csv", "json", "http", "api"
	Name    string            `json:"name"`              // Unique identifier for the source
	Path    string            `json:"path,omitempty"`    // Local file path
	URL     string            `json:"url,omitempty"`     // External HTTP API endpoint
	Headers map[string]string `json:"headers,omitempty"` // Request headers for HTTP sources
	Format  string            `json:"format,omitempty"`  // Data format ("csv", "json")
	// CSV only. HasHeader defaults to true; Columns names the columns (replacing the header, or
	// naming a headerless file such as the UCI Iris data); Delimiter defaults to ",".
	HasHeader *bool    `json:"has_header,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	Delimiter string   `json:"delimiter,omitempty"`
	// RecordsPath selects records inside a JSON document, e.g. "results" or "current_weather".
	RecordsPath string `json:"records_path,omitempty"`
}

// Rule defines validation and transformation constraints for a specific record field.
type Rule struct {
	Field     string   `json:"field"`               // Field name
	Required  bool     `json:"required,omitempty"`  // Must be non-empty if true
	Type      string   `json:"type,omitempty"`      // "string", "number", "boolean"
	Min       *float64 `json:"min,omitempty"`       // Minimum numeric threshold
	Max       *float64 `json:"max,omitempty"`       // Maximum numeric threshold
	Pattern   string   `json:"pattern,omitempty"`   // Regex pattern for validation
	Normalize string   `json:"normalize,omitempty"` // "trim", "lower", "upper", "title"
	Multiply  float64  `json:"multiply,omitempty"`  // Scaling factor for numeric values
	Add       float64  `json:"add,omitempty"`       // Offset addition for numeric values
	Default   any      `json:"default,omitempty"`   // Default fallback value if field is missing or empty
	Rename    string   `json:"rename,omitempty"`    // Rename the field after transformation (field mapping)
}

// ExportTarget defines where processed data should be persisted.
type ExportTarget struct {
	Type string `json:"type"`           // "json", "csv", "sqlite" (database file), "sql" (SQL dump)
	Path string `json:"path,omitempty"` // File destination path
}

// JobSpec specifies the complete specification for a pipeline execution job.
type JobSpec struct {
	Sources           []Source       `json:"sources"`
	Rules             []Rule         `json:"rules,omitempty"`
	Aggregations      []Aggregation  `json:"aggregations,omitempty"`
	Exports           []ExportTarget `json:"exports"`
	ValidationWorkers int            `json:"validation_workers,omitempty"`
	TransformWorkers  int            `json:"transform_workers,omitempty"`
	BufferSize        int            `json:"buffer_size,omitempty"`
	TimeoutSeconds    int            `json:"timeout_seconds,omitempty"`
	BatchSize         int            `json:"batch_size,omitempty"`
	// AggregationWorkers > 1 aggregates in parallel partials that are merged at the end.
	AggregationWorkers int `json:"aggregation_workers,omitempty"`
	// IncludeSourceField adds a "_source" field with the source name to every record, so mixed
	// sources can be grouped by origin (group_by: "_source").
	IncludeSourceField bool   `json:"include_source_field,omitempty"`
	MaxRecords         int    `json:"max_records,omitempty"`
	DLQPath            string `json:"dlq_path,omitempty"`
}

// RedactedHeaderValue replaces HTTP source header values in API responses and finished jobs.
const RedactedHeaderValue = "[REDACTED]"

// WithRedactedHeaders returns a copy of the spec whose HTTP source header values are replaced by
// RedactedHeaderValue. Headers often carry credentials for third-party APIs; they are needed
// only while the job runs and must never be echoed back to clients.
func (s JobSpec) WithRedactedHeaders() JobSpec {
	if len(s.Sources) == 0 {
		return s
	}
	sources := make([]Source, len(s.Sources))
	for i, src := range s.Sources {
		if len(src.Headers) > 0 {
			h := make(map[string]string, len(src.Headers))
			for k := range src.Headers {
				h[k] = RedactedHeaderValue
			}
			src.Headers = h
		}
		sources[i] = src
	}
	s.Sources = sources
	return s
}
