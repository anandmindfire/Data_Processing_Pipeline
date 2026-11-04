package models

// Record is an untyped dictionary representing a single data entity.
type Record map[string]any

// Clone creates a shallow clone of the record.
func (r Record) Clone() Record {
	clone := make(Record, len(r))
	for k, v := range r {
		clone[k] = v
	}
	return clone
}
