package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// ParseNumber converts various numeric types or strings into float64.
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

func checkRequired(field string, value any, ok bool) error {
	if !ok || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
		return fmt.Errorf("field %q is required", field)
	}
	return nil
}

func checkNumericRule(rule models.Rule, value any) error {
	n, isNum := ParseNumber(value)
	if !isNum {
		return fmt.Errorf("field %q must be numeric", rule.Field)
	}
	if rule.Min != nil && n < *rule.Min {
		return fmt.Errorf("field %q must be >= %v", rule.Field, *rule.Min)
	}
	if rule.Max != nil && n > *rule.Max {
		return fmt.Errorf("field %q must be <= %v", rule.Field, *rule.Max)
	}
	return nil
}

func checkStringRule(field string, value any) error {
	if _, isStr := value.(string); !isStr {
		return fmt.Errorf("field %q must be a string", field)
	}
	return nil
}

