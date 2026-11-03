package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// ValidateJobConfig checks that the submitted job config contains necessary definitions.
func ValidateJobConfig(c *models.Config) error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one input source is required")
	}
	for i, s := range c.Sources {
		if s.Type == "" {
			return fmt.Errorf("source[%d]: type is required", i)
		}
		if s.Name == "" {
			return fmt.Errorf("source[%d]: name is required", i)
		}
	}

	if len(c.Exports) == 0 {
		return fmt.Errorf("at least one export target is required")
	}
	for i, exp := range c.Exports {
		if exp.Type == "" {
			return fmt.Errorf("export[%d]: type is required", i)
		}
	}
	return nil
}

// writeJSON serializes data as JSON with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// writeError serializes an error message as JSON with the given HTTP status code.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}

