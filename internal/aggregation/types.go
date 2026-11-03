package aggregation

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// BuildAggKey returns the key identifier for an aggregation specification.
func BuildAggKey(a models.Aggregation) string {
	if a.Name != "" {
		return a.Name
	}
	return a.Operation + "_" + a.Field
}

