package aggregation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// buildFinalSummary computes finalized values (such as computing averages) and formats results.
func buildFinalSummary(definitions []models.Aggregation, aggregates map[string]map[string]float64, counts map[string]int64) map[string]any {
	results := make(map[string]any, len(aggregates))
	for _, def := range definitions {
		key := BuildAggKey(def)
		groupMap := make(map[string]float64, len(aggregates[key]))
		for group, val := range aggregates[key] {
			if strings.EqualFold(def.Operation, "avg") || strings.EqualFold(def.Operation, "average") {
				count := counts[key+"|"+group]
				if count > 0 {
					val = val / float64(count)
				}
			}
			groupMap[group] = val
		}
		results[key] = groupMap
	}
	return results
}
