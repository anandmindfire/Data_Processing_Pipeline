package ingestion

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Reader defines the behavior for ingesting records from configured sources.
type Reader interface {
	Read(ctx context.Context, source models.Source, maxRecords int, out chan<- Item, onDiscovered func()) error
}

// SourceReader is the concrete implementation of Reader.
type SourceReader struct {
	client       *http.Client
	allowPrivate bool
	maxBytes     int64
}

// ReaderOption customizes a SourceReader.
type ReaderOption func(*SourceReader)

// WithPrivateNetworks disables the private/loopback URL pre-check (for local development only).
func WithPrivateNetworks(allow bool) ReaderOption {
	return func(r *SourceReader) { r.allowPrivate = allow }
}

// WithMaxSourceBytes caps how many bytes a single source may supply (<= 0 = unlimited).
func WithMaxSourceBytes(n int64) ReaderOption {
	return func(r *SourceReader) { r.maxBytes = n }
}

// NewReader constructs a SourceReader. When client is nil an SSRF-safe client is used.
func NewReader(client *http.Client, opts ...ReaderOption) *SourceReader {
	r := &SourceReader{client: client}
	for _, opt := range opts {
		opt(r)
	}
	if r.client == nil {
		r.client = NewSafeHTTPClient(30*time.Second, r.allowPrivate)
	}
	return r
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
	body := newLimitedReader(stream, r.maxBytes)

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
		return readCSV(ctx, body, maxRecords, emit)
	case "json", "ndjson":
		if s.RecordsPath != "" {
			return readJSONAtPath(ctx, body, s.RecordsPath, maxRecords, emit)
		}
		return readJSON(ctx, body, maxRecords, emit)
	default:
		return fmt.Errorf("%w: format %q for source %q", ErrUnsupportedSourceType, format, s.Name)
	}
}

// openStream resolves and opens a source stream from either an HTTP endpoint or a local file.
func (r *SourceReader) openStream(ctx context.Context, s models.Source) (io.ReadCloser, error) {
	if s.URL != "" || strings.EqualFold(s.Type, "http") || strings.EqualFold(s.Type, "api") {
		return fetchHTTP(ctx, s, r.client, r.allowPrivate)
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

// resolveFormat infers the payload format (csv or json) from source format, path, or URL.
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
