package aggregation

import "strings"

// applyOperation updates the target accumulator with a new value using the requested operation.
func applyOperation(aggregates map[string]map[string]float64, key, group, op string, v float64) {
	switch strings.ToLower(op) {
	case "count":
		aggregates[key][group]++
	case "sum", "avg", "average":
		aggregates[key][group] += v
	case "min":
		curr, exists := aggregates[key][group]
		if !exists || v < curr {
			aggregates[key][group] = v
		}
	case "max":
		curr, exists := aggregates[key][group]
		if !exists || v > curr {
			aggregates[key][group] = v
		}
	}
}
