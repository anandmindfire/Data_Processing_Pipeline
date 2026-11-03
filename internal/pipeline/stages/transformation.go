package stages

import (
	"fmt"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// TransformRecord applies normalization and enrichment rules to a record immutably.
func TransformRecord(r models.Record, rules []models.Rule) (models.Record, error) {
	out := make(models.Record, len(r))
	for k, v := range r {
		out[k] = v
	}

	for _, rule := range rules {
		v, ok := out[rule.Field]
		if !ok || v == nil {
			continue
		}

		if strings.EqualFold(rule.Type, "number") {
			n, isNum := ParseNumber(v)
			if !isNum {
				return nil, fmt.Errorf("field %q cannot be converted to number", rule.Field)
			}
			if rule.Multiply != 0 {
				n *= rule.Multiply
			}
			out[rule.Field] = n
		}

		if rule.Normalize != "" {
			s := fmt.Sprint(out[rule.Field])
			switch strings.ToLower(rule.Normalize) {
			case "lower":
				out[rule.Field] = strings.ToLower(strings.TrimSpace(s))
			case "upper":
				out[rule.Field] = strings.ToUpper(strings.TrimSpace(s))
			case "trim":
				out[rule.Field] = strings.TrimSpace(s)
			}
		}
	}
	return out, nil
}

