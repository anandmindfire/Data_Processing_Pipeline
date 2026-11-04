package export

import "errors"

var (
	// ErrUnsupportedExportTarget indicates an unknown export target format.
	ErrUnsupportedExportTarget = errors.New("unsupported export target type")

	// ErrExportFailed indicates a failure during file creation or writing.
	ErrExportFailed = errors.New("export failed")
)
