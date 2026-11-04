package transformation

import "errors"

var (
	// ErrTransformationFailed indicates that a transformation rule could not be applied.
	ErrTransformationFailed = errors.New("transformation rule failed")
)
