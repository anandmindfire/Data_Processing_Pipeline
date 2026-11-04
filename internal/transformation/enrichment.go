package transformation

import (
	"fmt"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// applyNumericTransformation scales or offsets a numeric field according to rule.
func applyNumericTransformation(rule models.Rule, val any) (any, error) {
	n, isNum := validation.ParseNumber(val)
	if !isNum {
		return nil, fmt.Errorf("%w: field %q cannot be converted to number", ErrTransformationFailed, rule.Field)
	}
	if rule.Multiply != 0 {
		n *= rule.Multiply
	}
	if rule.Add != 0 {
		n += rule.Add
	}
	return n, nil
}

// applyDefaultEnrichment injects fallback values when a field is missing or empty.
func applyDefaultEnrichment(out models.Record, rule models.Rule) any {
	val, ok := out[rule.Field]
	if (!ok || val == nil || val == "") && rule.Default != nil {
		out[rule.Field] = rule.Default
		return rule.Default
	}
	return val
}
