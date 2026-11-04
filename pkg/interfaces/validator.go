package interfaces

// Validator defines the contract for validating data records against rules.
type Validator interface {
	Validate(record map[string]any, rules any) error
}
