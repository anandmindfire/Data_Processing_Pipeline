package transformation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// Transformer defines the behavior for applying normalization and transformations.
type Transformer interface {
	Transform(record models.Record, rules []models.Rule) (models.Record, error)
}

// RecordTransformer is the concrete implementation of Transformer.
type RecordTransformer struct{}

// NewTransformer constructs a new RecordTransformer.
func NewTransformer() *RecordTransformer {
	return &RecordTransformer{}
}

// Transform applies normalization and enrichment rules immutably to a record.
func (t *RecordTransformer) Transform(r models.Record, rules []models.Rule) (models.Record, error) {
	out := r.Clone()

	for _, rule := range rules {
		val := applyDefaultEnrichment(out, rule)
		if val == nil {
			continue
		}
		if validation.IsBlank(val) {
			// A blank cell in a typed field becomes null instead of an invalid number/boolean.
			if isTyped(rule) {
				out[rule.Field] = nil
			}
			continue
		}

		if strings.EqualFold(rule.Type, "number") {
			transformedNum, err := applyNumericTransformation(rule, val)
			if err != nil {
				return nil, err
			}
			out[rule.Field] = transformedNum
		}
		if strings.EqualFold(rule.Type, "boolean") || strings.EqualFold(rule.Type, "bool") {
			b, err := convertBool(rule, val)
			if err != nil {
				return nil, err
			}
			out[rule.Field] = b
		}

		if rule.Normalize != "" {
			out[rule.Field] = applyStringNormalization(rule.Normalize, out[rule.Field])
		}
	}

	// Field mapping runs last so every rule above can refer to the original field names.
	for _, rule := range rules {
		if rule.Rename == "" || rule.Rename == rule.Field {
			continue
		}
		if v, ok := out[rule.Field]; ok {
			out[rule.Rename] = v
			delete(out, rule.Field)
		}
	}
	return out, nil
}

// isTyped reports whether a rule converts its field to a number or boolean.
func isTyped(rule models.Rule) bool {
	t := strings.ToLower(rule.Type)
	return t == "number" || t == "boolean" || t == "bool"
}
