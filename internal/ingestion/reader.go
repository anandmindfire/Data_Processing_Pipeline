package ingestion

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Reader defines the behavior for ingesting records from configured sources.
type Reader interface {
	Read(ctx context.Context, source models.Source, maxRecords int, out chan<- Item, onDiscovered func()) error
}

// SourceReader is the concrete implementation of Reader.
type SourceReader struct {
	client *http.Client
}

// NewReader constructs a new SourceReader with the provided HTTP client.
func NewReader(client *http.Client) *SourceReader {
	if client == nil {
		client = http.DefaultClient
	}
	return &SourceReader{client: client}
}

// Read opens the source stream, parses records according to format, and emits Item instances.
func (r *SourceReader) Read(ctx context.Context, s models.Source, maxRecords int, out chan<- Item, onDiscovered func()) error {
	stream, err := r.openStream(ctx, s)
	if err != nil {
		return err
	}
	defer func() {
		_ = stream.Close()
	}()

	emit := func(index int, rec models.Record) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- Item{Source: s.Name, Index: index, Record: rec}:
			if onDiscovered != nil {
				onDiscovered()
			}
			return nil
		}
	}

	format := resolveFormat(s)
	switch format {
	case "csv":
		return readCSV(ctx, stream, maxRecords, emit)
	case "json":
		return readJSON(ctx, stream, maxRecords, emit)
	default:
		return fmt.Errorf("%w: format %q for source %q", ErrUnsupportedSourceType, format, s.Name)
	}
}

func (r *SourceReader) openStream(ctx context.Context, s models.Source) (io.ReadCloser, error) {
	if s.URL != "" || strings.EqualFold(s.Type, "http") || strings.EqualFold(s.Type, "api") {
		return fetchHTTP(ctx, s, r.client)
	}

	if s.Path != "" {
		f, err := os.Open(s.Path)
		if err != nil {
			return nil, fmt.Errorf("open source %q (%s): %w", s.Name, s.Path, err)
		}
		return f, nil
	}

	return nil, fmt.Errorf("source %q: %w", s.Name, ErrSourceRequired)
}

func resolveFormat(s models.Source) string {
	format := strings.ToLower(s.Format)
	if format != "" {
		return format
	}

	target := strings.ToLower(s.Path)
	if target == "" {
		target = strings.ToLower(s.URL)
	}

	if strings.HasSuffix(target, ".csv") || strings.EqualFold(s.Type, "csv") {
		return "csv"
	}
	if strings.HasSuffix(target, ".json") || strings.EqualFold(s.Type, "json") {
		return "json"
	}

	if strings.EqualFold(s.Type, "http") || strings.EqualFold(s.Type, "api") {
		return "json"
	}
	return strings.ToLower(s.Type)
}
