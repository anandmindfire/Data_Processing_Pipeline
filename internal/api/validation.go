package api

import (
	"fmt"
	"strings"

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
		if s.Path == "" && s.URL == "" {
			return fmt.Errorf("source[%d] %q: either path or url is required", i, s.Name)
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

	for i, agg := range c.Aggregations {
		if agg.Field == "" {
			return fmt.Errorf("aggregation[%d]: field is required", i)
		}
		if agg.Operation == "" {
			return fmt.Errorf("aggregation[%d]: operation is required", i)
		}
		switch strings.ToLower(agg.Operation) {
		case "count", "sum", "avg", "average", "min", "max":
		default:
			return fmt.Errorf("aggregation[%d]: unsupported operation %q", i, agg.Operation)
		}
	}
	return nil
}
