package validation

import "errors"

var (
	// ErrRequiredField indicates that a mandatory field is missing or empty.
	ErrRequiredField = errors.New("required field is missing or empty")

	// ErrTypeMismatch indicates that a field value does not conform to the expected data type.
	ErrTypeMismatch = errors.New("field type mismatch")

	// ErrNumericConstraint indicates that a numeric value violated minimum or maximum bounds.
	ErrNumericConstraint = errors.New("numeric constraint violation")

	// ErrPatternMismatch indicates that a string value does not match the configured regular expression.
	ErrPatternMismatch = errors.New("string pattern mismatch")
)
