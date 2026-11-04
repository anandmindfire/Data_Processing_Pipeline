package models

// Aggregation defines an aggregation calculation to compute over processed records.
type Aggregation struct {
	GroupBy   string `json:"group_by,omitempty"` // Optional field to group records by
	Field     string `json:"field"`              // Target field to compute aggregation on
	Operation string `json:"operation"`          // "count", "sum", "avg", "average"
	Name      string `json:"name,omitempty"`     // Custom alias name for output
}

// AggregationResult represents the computed result of an aggregation.
type AggregationResult struct {
	Name      string         `json:"name"`
	Operation string         `json:"operation"`
	Field     string         `json:"field"`
	GroupBy   string         `json:"group_by,omitempty"`
	Value     any            `json:"value,omitempty"`
	Groups    map[string]any `json:"groups,omitempty"`
}
