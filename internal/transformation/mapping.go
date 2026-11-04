package transformation

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// cloneRecord creates a shallow clone of the source record map.
func cloneRecord(r models.Record) models.Record {
	cloned := make(models.Record, len(r))
	for k, v := range r {
		cloned[k] = v
	}
	return cloned
}

// MapField renames or maps an existing field key to a target key.
func MapField(r models.Record, srcKey, targetKey string) {
	if val, ok := r[srcKey]; ok {
		r[targetKey] = val
		delete(r, srcKey)
	}
}
