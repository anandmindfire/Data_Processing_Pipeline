package stages

import (
	"fmt"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Aggregator accumulates metrics and computes final summaries (sum, count, avg).
type Aggregator struct {
	definitions []models.Aggregation
	aggregates  map[string]map[string]float64
	counts      map[string]int64
}

// NewAggregator creates an initialized Aggregator for the given aggregation definitions.
func NewAggregator(definitions []models.Aggregation) *Aggregator {
	aggs := make(map[string]map[string]float64, len(definitions))
	for _, a := range definitions {
		key := AggKey(a)
		aggs[key] = make(map[string]float64)
	}
	return &Aggregator{
		definitions: definitions,
		aggregates:  aggs,
		counts:      make(map[string]int64),
	}
}

// AggKey returns the unique metric name for an Aggregation definition.
func AggKey(a models.Aggregation) string {
	if a.Name != "" {
		return a.Name
	}
	return a.Operation + "_" + a.Field
}

// Process consumes a single transformed record and updates accumulators.
func (a *Aggregator) Process(r models.Record) {
	for _, def := range a.definitions {
		key := AggKey(def)
		group := "_all"
		if def.GroupBy != "" {
			group = fmt.Sprint(r[def.GroupBy])
		}

		v, isNum := ParseNumber(r[def.Field])
		if !isNum && !strings.EqualFold(def.Operation, "count") {
			continue
		}

		switch strings.ToLower(def.Operation) {
		case "count":
			a.aggregates[key][group]++
		case "sum", "avg", "average":
			a.aggregates[key][group] += v
		}
		a.counts[key+"|"+group]++
	}
}

// Finalize calculates final values (e.g. dividing sum by count for averages) and returns the aggregates map.
func (a *Aggregator) Finalize() map[string]any {
	result := make(map[string]any, len(a.aggregates))
	for _, def := range a.definitions {
		key := AggKey(def)
		groupMap := make(map[string]float64, len(a.aggregates[key]))
		for group, val := range a.aggregates[key] {
			if strings.EqualFold(def.Operation, "avg") || strings.EqualFold(def.Operation, "average") {
				count := a.counts[key+"|"+group]
				if count > 0 {
					val = val / float64(count)
				}
			}
			groupMap[group] = val
		}
		result[key] = groupMap
	}
	return result
}

