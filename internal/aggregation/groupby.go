package aggregation

import (
	"fmt"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// BuildAggKey returns the key identifier for an aggregation specification.
func BuildAggKey(a models.Aggregation) string {
	if a.Name != "" {
		return a.Name
	}
	return a.Operation + "_" + a.Field
}

// resolveGroup extracts the group identifier from a record based on group-by definition.
func resolveGroup(r models.Record, groupBy string) string {
	if groupBy == "" {
		return "_all"
	}
	if grpVal, ok := r[groupBy]; ok && grpVal != nil {
		return fmt.Sprint(grpVal)
	}
	return "unknown"
}
