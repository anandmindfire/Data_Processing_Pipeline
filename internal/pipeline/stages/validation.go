package stages

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// ValidateRecord evaluates a record against the supplied rules.
func ValidateRecord(r models.Record, rules []models.Rule) error {
	for _, rule := range rules {
		v, ok := r[rule.Field]
		if rule.Required && (!ok || v == nil || strings.TrimSpace(fmt.Sprint(v)) == "") {
			return fmt.Errorf("field %q is required", rule.Field)
		}
		if !ok || v == nil {
			continue
		}

		switch strings.ToLower(rule.Type) {
		case "number":
			n, isNum := ParseNumber(v)
			if !isNum {
				return fmt.Errorf("field %q must be numeric", rule.Field)
			}
			if rule.Min != nil && n < *rule.Min {
				return fmt.Errorf("field %q must be >= %v", rule.Field, *rule.Min)
			}
			if rule.Max != nil && n > *rule.Max {
				return fmt.Errorf("field %q must be <= %v", rule.Field, *rule.Max)
			}
		case "string":
			if _, isStr := v.(string); !isStr {
				return fmt.Errorf("field %q must be a string", rule.Field)
			}
		}
	}
	return nil
}

// ParseNumber converts various types into a float64.
func ParseNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

