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
		return fmt.Errorf("unsupported format %q for source %q", format, s.Name)
	}
}

func (r *SourceReader) openStream(ctx context.Context, s models.Source) (io.ReadCloser, error) {
	switch strings.ToLower(s.Type) {
	case "csv", "json":
		if s.Path == "" {
			return nil, fmt.Errorf("source %q: path required", s.Name)
		}
		return os.Open(s.Path)
	case "http", "api":
		return fetchHTTP(ctx, s, r.client)
	default:
		return nil, fmt.Errorf("unsupported source type %q", s.Type)
	}
}

func resolveFormat(s models.Source) string {
	format := strings.ToLower(s.Format)
	if format == "" {
		format = strings.ToLower(s.Type)
		if format == "http" || format == "api" {
			format = "json"
		}
	}
	return format
}

