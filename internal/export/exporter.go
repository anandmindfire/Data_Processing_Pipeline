package export

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Exporter defines the behavior for writing processed records to target sinks.
type Exporter interface {
	Export(records []models.Record, targets []models.ExportTarget, jobID string) (string, error)
}

// FileExporter is the concrete implementation of Exporter writing to local storage.
type FileExporter struct {
	defaultDir string
}

// NewExporter creates a new FileExporter instance with fallback destination directory.
func NewExporter(defaultDir string) *FileExporter {
	if defaultDir == "" {
		defaultDir = "exports"
	}
	return &FileExporter{defaultDir: defaultDir}
}

// Export serializes records to the targets specified and returns the primary output path.
func (e *FileExporter) Export(records []models.Record, targets []models.ExportTarget, jobID string) (string, error) {
	primaryPath := ""

	for _, target := range targets {
		if target.Type == "" {
			continue
		}

		path := e.resolvePath(target, jobID)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return "", fmt.Errorf("create export directory: %w", err)
		}

		f, err := os.Create(path)
		if err != nil {
			return "", fmt.Errorf("create export file %q: %w", path, err)
		}

		err = writeToTarget(f, target.Type, records)
		closeErr := f.Close()
		if err != nil {
			return "", err
		}
		if closeErr != nil {
			return "", closeErr
		}

		if primaryPath == "" {
			primaryPath = path
		}
	}
	return primaryPath, nil
}

func (e *FileExporter) resolvePath(target models.ExportTarget, jobID string) string {
	if target.Path != "" {
		return target.Path
	}
	return filepath.Join(e.defaultDir, jobID+"."+strings.ToLower(target.Type))
}

func writeToTarget(f *os.File, targetType string, records []models.Record) error {
	switch strings.ToLower(targetType) {
	case "json":
		return writeJSON(f, records)
	case "csv":
		return writeCSV(f, records)
	default:
		return fmt.Errorf("unsupported export target type %q", targetType)
	}
}

