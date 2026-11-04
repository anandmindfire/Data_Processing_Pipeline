package types

// Metadata represents generic key-value metadata.
type Metadata map[string]any

// ID represents an entity identifier.
type ID = string

// KeyValue represents a single string key-value pair.
type KeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}
