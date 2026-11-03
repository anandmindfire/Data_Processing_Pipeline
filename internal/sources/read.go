package sources

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"io"
	"net/http"
	"os"
	"strings"
)

type Item struct {
	Source string
	Index  int
	Record domain.Record
}

func Read(ctx context.Context, s domain.Source, client *http.Client, max int, out chan<- Item, discovered func()) error {
	var r io.ReadCloser
	var err error
	switch strings.ToLower(s.Type) {
	case "csv", "json":
		if s.Path == "" {
			return fmt.Errorf("source %q: path required", s.Name)
		}
		r, err = os.Open(s.Path)
	case "http", "api":
		if s.URL == "" {
			return fmt.Errorf("source %q: url required", s.Name)
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
		if e != nil {
			return e
		}
		for k, v := range s.Headers {
			req.Header.Set(k, v)
		}
		resp, e := client.Do(req)
		if e != nil {
			return e
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return fmt.Errorf("source %q: HTTP %d", s.Name, resp.StatusCode)
		}
		r = resp.Body
	default:
		return fmt.Errorf("unsupported source type %q", s.Type)
	}
	if err != nil {
		return err
	}
	defer r.Close()
	format := strings.ToLower(s.Format)
	if format == "" {
		format = strings.ToLower(s.Type)
		if format == "http" || format == "api" {
			format = "json"
		}
	}
	emit := func(i int, v domain.Record) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- Item{Source: s.Name, Index: i, Record: v}:
			discovered()
			return nil
		}
	}
	switch format {
	case "csv":
		cr := csv.NewReader(r)
		cr.TrimLeadingSpace = true
		headers, e := cr.Read()
		if e != nil {
			return e
		}
		for i := 0; max <= 0 || i < max; i++ {
			row, e := cr.Read()
			if e == io.EOF {
				return nil
			}
			if e != nil {
				return fmt.Errorf("csv row %d: %w", i+2, e)
			}
			rec := domain.Record{}
			for n, h := range headers {
				if n < len(row) {
					rec[strings.TrimSpace(h)] = strings.TrimSpace(row[n])
				}
			}
			if e = emit(i+1, rec); e != nil {
				return e
			}
		}
	case "json":
		dec := json.NewDecoder(r)
		var raw json.RawMessage
		if e := dec.Decode(&raw); e != nil {
			return e
		}
		var arr []map[string]any
		if e := json.Unmarshal(raw, &arr); e == nil {
			for i, v := range arr {
				if max > 0 && i >= max {
					break
				}
				if e = emit(i+1, domain.Record(v)); e != nil {
					return e
				}
			}
			return nil
		}
		var obj map[string]any
		if e := json.Unmarshal(raw, &obj); e == nil {
			return emit(1, domain.Record(obj))
		}
		return fmt.Errorf("JSON input must be an object or array of objects")
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
	return nil
}
