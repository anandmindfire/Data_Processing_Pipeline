package transformation

import (
	"fmt"
	"strings"
	"unicode"

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

// applyStringNormalization applies trim, lower, upper, or title string transformations.
func applyStringNormalization(normalization string, val any) string {
	s := fmt.Sprint(val)
	switch strings.ToLower(normalization) {
	case "lower":
		return strings.ToLower(strings.TrimSpace(s))
	case "upper":
		return strings.ToUpper(strings.TrimSpace(s))
	case "trim":
		return strings.TrimSpace(s)
	case "title":
		return toTitleCase(strings.TrimSpace(s))
	default:
		return s
	}
}

func toTitleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		r := []rune(strings.ToLower(w))
		if len(r) > 0 {
			r[0] = unicode.ToUpper(r[0])
		}
		words[i] = string(r)
	}
	return strings.Join(words, " ")
}
