package transformation

import (
	"fmt"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// applyNumericTransformation scales a numeric field according to rule.
func applyNumericTransformation(rule models.Rule, val any) (any, error) {
	n, isNum := validation.ParseNumber(val)
	if !isNum {
		return nil, fmt.Errorf("field %q cannot be converted to number", rule.Field)
	}
	if rule.Multiply != 0 {
		n *= rule.Multiply
	}
	return n, nil
}

// applyStringNormalization applies trim, lower, or upper string transformations.
func applyStringNormalization(normalization string, val any) string {
	s := fmt.Sprint(val)
	switch strings.ToLower(normalization) {
	case "lower":
		return strings.ToLower(strings.TrimSpace(s))
	case "upper":
		return strings.ToUpper(strings.TrimSpace(s))
	case "trim":
		return strings.TrimSpace(s)
	default:
		return s
	}
}

