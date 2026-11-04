package interfaces

// Aggregator defines the contract for streaming record aggregation.
type Aggregator interface {
	Process(record map[string]any)
	Results() map[string]any
}
