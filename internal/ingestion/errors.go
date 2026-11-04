package ingestion

import "errors"

var (
	// ErrSourceNotFound indicates that a referenced local source file does not exist.
	ErrSourceNotFound = errors.New("source file not found")

	// ErrUnsupportedSourceType indicates that an unsupported source protocol or type was configured.
	ErrUnsupportedSourceType = errors.New("unsupported source type")

	// ErrInvalidFormat indicates that the stream cannot be parsed into records.
	ErrInvalidFormat = errors.New("invalid data format")

	// ErrHTTPFetchFailed indicates that an HTTP GET request to a remote source failed.
	ErrHTTPFetchFailed = errors.New("http fetch failed")

	// ErrSourceRequired indicates that neither path nor URL was specified for a source.
	ErrSourceRequired = errors.New("source requires either path or url")
)
