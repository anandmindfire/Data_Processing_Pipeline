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

func checkNumericRule(rule models.Rule, value any) error {
	n, isNum := ParseNumber(value)
	if !isNum {
		return fmt.Errorf("field %q: invalid number %q: %w", rule.Field, fmt.Sprint(value), ErrTypeMismatch)
	}
	if rule.Min != nil && n < *rule.Min {
		return fmt.Errorf("field %q: value %v below minimum %v: %w", rule.Field, n, *rule.Min, ErrNumericConstraint)
	}
	if rule.Max != nil && n > *rule.Max {
		return fmt.Errorf("field %q: value %v exceeds maximum %v: %w", rule.Field, n, *rule.Max, ErrNumericConstraint)
	}
	return nil
}

func (v *RecordValidator) checkStringRule(rule models.Rule, value any) error {
	s, isStr := value.(string)
	if !isStr {
		return fmt.Errorf("field %q: %w (expected string)", rule.Field, ErrTypeMismatch)
	}
	if rule.Pattern != "" {
		re, err := v.getOrCompileRegex(rule.Pattern)
		if err != nil {
			return fmt.Errorf("field %q: invalid regex %q: %w", rule.Field, rule.Pattern, err)
		}
		if !re.MatchString(s) {
			return fmt.Errorf("field %q: value %q does not match pattern %q: %w", rule.Field, s, rule.Pattern, ErrPatternMismatch)
		}
	}
	return nil
}

func checkBooleanRule(rule models.Rule, value any) error {
	_, isBool := ParseBool(value)
	if !isBool {
		return fmt.Errorf("field %q: invalid boolean %q: %w", rule.Field, fmt.Sprint(value), ErrTypeMismatch)
	}
	return nil
}
