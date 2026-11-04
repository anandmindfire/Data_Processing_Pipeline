package aggregation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
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
		group := resolveGroup(r, def.GroupBy)

		v, isNum := validation.ParseNumber(r[def.Field])
		if !isNum && !strings.EqualFold(def.Operation, "count") {
			continue
		}

		applyOperation(a.aggregates, key, group, def.Operation, v)
		a.counts[key+"|"+group]++
	}
}

// Finalize calculates final summary metrics (averages) and returns results map.
func (a *MetricsAggregator) Finalize() map[string]any {
	return buildFinalSummary(a.definitions, a.aggregates, a.counts)
}
