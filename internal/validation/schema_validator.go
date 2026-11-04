package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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

// ParseBool parses boolean values from bool types or truthy/falsy strings.
func ParseBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(x))
		return b, err == nil
	}
	return false, false
}

func checkRequired(field string, value any, ok bool) error {
	if !ok || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
		return fmt.Errorf("field %q: %w", field, ErrRequiredField)
	}
	return nil
}

func (v *RecordValidator) validateFieldType(rule models.Rule, val any) error {
	switch strings.ToLower(rule.Type) {
	case "number":
		return checkNumericRule(rule, val)
	case "string":
		return v.checkStringRule(rule, val)
	case "bool", "boolean":
		return checkBooleanRule(rule, val)
	default:
		return nil
	}
}
