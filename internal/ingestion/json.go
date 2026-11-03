package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// readJSON decodes either a JSON array of objects or a single JSON object.
func readJSON(ctx context.Context, r io.Reader, max int, emit func(int, models.Record) error) error {
	var raw json.RawMessage
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return fmt.Errorf("decode json payload: %w", err)
	}

	var array []map[string]any
	if err := json.Unmarshal(raw, &array); err == nil {
		return emitJSONArray(ctx, array, max, emit)
	}

	var single map[string]any
	if err := json.Unmarshal(raw, &single); err == nil {
		return emit(1, models.Record(single))
	}

	return fmt.Errorf("json must be an object or an array of objects")
}

func emitJSONArray(ctx context.Context, arr []map[string]any, max int, emit func(int, models.Record) error) error {
	for i, obj := range arr {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if max > 0 && i >= max {
			break
		}
		if err := emit(i+1, models.Record(obj)); err != nil {
			return err
		}
	}
	return nil
}

