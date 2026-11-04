package transformation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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
	out := cloneRecord(r)

	for _, rule := range rules {
		val := applyDefaultEnrichment(out, rule)
		if val == nil {
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
	return out, nil
}
