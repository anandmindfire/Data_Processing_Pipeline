package interfaces

// Transformer defines the contract for transforming data records according to rules.
type Transformer interface {
	Transform(record map[string]any, rules any) (map[string]any, error)
}
