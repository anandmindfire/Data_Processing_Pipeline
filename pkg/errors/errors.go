package errors

import "errors"

var (
	ErrCapacityReached         = errors.New("pipeline capacity reached; max active jobs running")
	ErrJobNotFound             = errors.New("pipeline job not found")
	ErrJobStillRunning         = errors.New("cannot delete an actively running job; cancel it first")
	ErrJobNotRunning           = errors.New("pipeline job is not running")
	ErrRequiredField           = errors.New("required field missing or empty")
	ErrTypeMismatch            = errors.New("field type mismatch")
	ErrNumericConstraint       = errors.New("value violates numeric constraint")
	ErrPatternMismatch         = errors.New("value does not match pattern")
	ErrUnsupportedSourceType   = errors.New("unsupported source type")
	ErrSourceRequired          = errors.New("either path or url must be provided")
	ErrInvalidFormat           = errors.New("invalid file format")
	ErrHTTPFetchFailed         = errors.New("failed to fetch from HTTP source")
	ErrTransformationFailed    = errors.New("transformation rule failed")
	ErrUnsupportedExportTarget = errors.New("unsupported export target type")
	ErrExportFailed            = errors.New("failed to export records")
)
