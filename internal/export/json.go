package export

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// writeJSON serializes records as formatted JSON into the given writer.
func writeJSON(w io.Writer, records []models.Record) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(records); err != nil {
		return fmt.Errorf("encode json export: %w", err)
	}
	return nil
}
