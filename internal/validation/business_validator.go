package validation

import (
	"fmt"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

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
