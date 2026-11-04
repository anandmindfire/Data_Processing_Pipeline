package aggregation

import "errors"

var (
	// ErrUnsupportedOperation indicates that an unknown aggregation operation was requested.
	ErrUnsupportedOperation = errors.New("unsupported aggregation operation")
)
