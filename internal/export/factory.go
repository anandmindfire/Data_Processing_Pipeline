package export

import (
	"fmt"
	"io"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// TargetWriter is a function that formats and writes records to an io.Writer.
type TargetWriter func(w io.Writer, records []models.Record) error

// ExporterFactory creates target writers according to the export type.
type ExporterFactory struct{}

// NewExporterFactory constructs a new ExporterFactory.
func NewExporterFactory() *ExporterFactory {
	return &ExporterFactory{}
}

// GetWriter resolves a TargetWriter for the given target type.
func (f *ExporterFactory) GetWriter(targetType string) (TargetWriter, error) {
	switch strings.ToLower(strings.TrimSpace(targetType)) {
	case "json":
		return writeJSON, nil
	case "csv":
		return writeCSV, nil
	case "sqlite", "sql":
		return writeSQLite, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedExportTarget, targetType)
	}
}
