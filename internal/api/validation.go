package api

import (
	"fmt"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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
