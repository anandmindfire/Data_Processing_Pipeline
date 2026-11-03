package aggregation

import (
	"fmt"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/validation"
)

// Aggregator defines the behavior for accumulating and finalizing aggregate metrics.
type Aggregator interface {
	Process(record models.Record)
	Finalize() map[string]any
}

// MetricsAggregator is the concrete implementation of Aggregator.
type MetricsAggregator struct {
	definitions []models.Aggregation
	aggregates  map[string]map[string]float64
	counts      map[string]int64
}

// NewAggregator creates an initialized MetricsAggregator for the given definitions.
func NewAggregator(definitions []models.Aggregation) *MetricsAggregator {
	aggs := make(map[string]map[string]float64, len(definitions))
	for _, def := range definitions {
		key := BuildAggKey(def)
		aggs[key] = make(map[string]float64)
	}
	return &MetricsAggregator{
		definitions: definitions,
		aggregates:  aggs,
		counts:      make(map[string]int64),
	}
}

// Process consumes a single transformed record and updates accumulators.
func (a *MetricsAggregator) Process(r models.Record) {
	for _, def := range a.definitions {
		key := BuildAggKey(def)
		group := "_all"
		if def.GroupBy != "" {
			group = fmt.Sprint(r[def.GroupBy])
		}

		v, isNum := validation.ParseNumber(r[def.Field])
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

// Finalize calculates final summary metrics (averages) and returns results map.
func (a *MetricsAggregator) Finalize() map[string]any {
	results := make(map[string]any, len(a.aggregates))
	for _, def := range a.definitions {
		key := BuildAggKey(def)
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
		results[key] = groupMap
	}
	return results
}

