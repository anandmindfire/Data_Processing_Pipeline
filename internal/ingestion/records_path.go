package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// readJSONAtPath decodes a JSON document and emits the records found at a dot-separated path.
//
// Many REST APIs wrap their data, e.g. {"results":[...]} (RandomUser) or
// {"current_weather":{...}} (Open-Meteo). The value at the path may be an array of objects (one
// record each) or a single object (one record). Numeric segments index into arrays ("data.0").
// The whole document is held in memory, which is bounded by the source size limit.
func readJSONAtPath(ctx context.Context, r io.Reader, path string, max int, emit func(int, models.Record) error) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("%w: decode document: %w", ErrInvalidFormat, err)
	}
	node, err := lookupPath(doc, path)
	if err != nil {
		return err
	}

	switch v := node.(type) {
	case map[string]any:
		return emit(1, normalizeNumbers(v))
	case []any:
		return emitArray(ctx, v, path, max, emit)
	default:
		return fmt.Errorf("%w: records_path %q does not point to an object or array", ErrInvalidFormat, path)
	}
}

// lookupPath walks a dot-separated path through objects (by key) and arrays (by index).
func lookupPath(doc any, path string) (any, error) {
	node := doc
	for _, seg := range strings.Split(path, ".") {
		switch v := node.(type) {
		case map[string]any:
			next, ok := v[seg]
			if !ok {
				return nil, fmt.Errorf("%w: records_path %q: key %q not found", ErrInvalidFormat, path, seg)
			}
			node = next
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(v) {
				return nil, fmt.Errorf("%w: records_path %q: invalid array index %q", ErrInvalidFormat, path, seg)
			}
			node = v[i]
		default:
			return nil, fmt.Errorf("%w: records_path %q: cannot descend into a scalar at %q", ErrInvalidFormat, path, seg)
		}
	}
	return node, nil
}

// emitArray emits each object of an array as a record, honouring max_records and cancellation.
func emitArray(ctx context.Context, items []any, path string, max int, emit func(int, models.Record) error) error {
	for idx, item := range items {
		if max > 0 && idx >= max {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		obj, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: records_path %q: array element %d is not an object", ErrInvalidFormat, path, idx+1)
		}
		if err := emit(idx+1, normalizeNumbers(obj)); err != nil {
			return err
		}
	}
	return nil
}

// normalizeNumbers converts json.Number values to float64 so records look the same as those
// produced by the streaming JSON reader.
func normalizeNumbers(m map[string]any) models.Record {
	for k, v := range m {
		m[k] = normalizeValue(v)
	}
	return models.Record(m)
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case map[string]any:
		for k, e := range x {
			x[k] = normalizeValue(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = normalizeValue(e)
		}
		return x
	default:
		return v
	}
}
