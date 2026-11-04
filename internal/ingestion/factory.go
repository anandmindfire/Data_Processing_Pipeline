package ingestion

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// StreamParser is a function that parses a stream of bytes into records.
type StreamParser func(ctx context.Context, r io.Reader, max int, emit func(int, models.Record) error) error

// ReaderFactory resolves the appropriate stream parser based on the data format.
type ReaderFactory struct{}

// NewReaderFactory returns a new instance of ReaderFactory.
func NewReaderFactory() *ReaderFactory {
	return &ReaderFactory{}
}

// GetParser returns a StreamParser for the requested format ("csv", "json", etc.).
func (f *ReaderFactory) GetParser(format string) (StreamParser, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "csv":
		return readCSV, nil
	case "json", "ndjson":
		return readJSON, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedSourceType, format)
	}
}
