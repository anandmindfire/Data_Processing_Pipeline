package aggregation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// Aggregator defines the behavior for accumulating and finalizing aggregate metrics.
//
// An Aggregator is not safe for concurrent use. To aggregate in parallel, give each worker its
// own partial aggregator (NewAggregator) and Merge the partials once the workers finish.
type Aggregator interface {
	Process(record models.Record)
	Merge(other Aggregator)
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

		raw, present := r[def.Field]
		// count behaves like SQL COUNT(field): only records where the field has a value. In jobs
		// that mix sources this keeps records from other sources out of the groups.
		if !present || validation.IsBlank(raw) {
			continue
		}
		v, isNum := validation.ParseNumber(raw)
		if !isNum && !strings.EqualFold(def.Operation, "count") {
			continue
		}

		applyOperation(a.aggregates, key, group, def.Operation, v)
		a.counts[key+"|"+group]++
	}
}

// Merge folds another partial aggregator built from the same definitions into a. Every supported
// operation is associative (count/sum/avg add, min/max compare), so merging partials gives the
// same result as aggregating sequentially.
func (a *MetricsAggregator) Merge(other Aggregator) {
	o, ok := other.(*MetricsAggregator)
	if !ok || o == nil {
		return
	}
	ops := make(map[string]string, len(a.definitions))
	for _, def := range a.definitions {
		ops[BuildAggKey(def)] = strings.ToLower(def.Operation)
	}
	for key, groups := range o.aggregates {
		dst, ok := a.aggregates[key]
		if !ok {
			dst = make(map[string]float64, len(groups))
			a.aggregates[key] = dst
		}
		for group, v := range groups {
			cur, exists := dst[group]
			switch ops[key] {
			case "min":
				if !exists || v < cur {
					dst[group] = v
				}
			case "max":
				if !exists || v > cur {
					dst[group] = v
				}
			default: // count, sum, avg/average
				dst[group] = cur + v
			}
		}
	}
	for k, n := range o.counts {
		a.counts[k] += n
	}
}

// Finalize calculates final summary metrics (averages) and returns results map.
func (a *MetricsAggregator) Finalize() map[string]any {
	return buildFinalSummary(a.definitions, a.aggregates, a.counts)
}
