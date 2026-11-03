package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// readJSON decodes a JSON payload (either an array of objects or a single object) and emits records.
func readJSON(ctx context.Context, sourceName string, r io.Reader, max int, out chan<- Item, emit func(int, models.Record) error) error {
	dec := json.NewDecoder(r)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("decode json payload: %w", err)
	}

	// Try decoding as array of objects
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err == nil {
		for i, obj := range arr {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if max > 0 && i >= max {
				break
			}
			if err = emit(i+1, models.Record(obj)); err != nil {
				return err
			}
		}
		return nil
	}

	// Try decoding as a single object
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		return emit(1, models.Record(obj))
	}

	return fmt.Errorf("JSON input must be an object or an array of objects")
}

