package sources

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Item wraps a parsed record with source tracking information.
type Item struct {
	Source string
	Index  int
	Record models.Record
}

// Read ingests data from the specified Source definition, parsing records into the out channel.
func Read(ctx context.Context, s models.Source, client *http.Client, max int, out chan<- Item, discovered func()) error {
	var r io.ReadCloser
	var err error

	switch strings.ToLower(s.Type) {
	case "csv", "json":
		if s.Path == "" {
			return fmt.Errorf("source %q: path required", s.Name)
		}
		r, err = os.Open(s.Path)
	case "http", "api":
		r, err = fetchHTTP(ctx, s, client)
	default:
		return fmt.Errorf("unsupported source type %q", s.Type)
	}

	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()

	format := strings.ToLower(s.Format)
	if format == "" {
		format = strings.ToLower(s.Type)
		if format == "http" || format == "api" {
			format = "json"
		}
	}

	emit := func(i int, v models.Record) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- Item{Source: s.Name, Index: i, Record: v}:
			if discovered != nil {
				discovered()
			}
			return nil
		}
	}

	switch format {
	case "csv":
		return readCSV(ctx, s.Name, r, max, out, emit)
	case "json":
		return readJSON(ctx, s.Name, r, max, out, emit)
	default:
		return fmt.Errorf("unsupported format %q for source %q", format, s.Name)
	}
}
