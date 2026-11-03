package validation

import (
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Validator defines the behavior for validating a record against rules.
type Validator interface {
	Validate(record models.Record, rules []models.Rule) error
}

// RecordValidator is the concrete implementation of Validator.
type RecordValidator struct{}

// NewValidator constructs a new RecordValidator.
func NewValidator() *RecordValidator {
	return &RecordValidator{}
}

// Validate verifies that the record satisfies all specified rules.
func (v *RecordValidator) Validate(r models.Record, rules []models.Rule) error {
	for _, rule := range rules {
		val, ok := r[rule.Field]
		if rule.Required {
			if err := checkRequired(rule.Field, val, ok); err != nil {
				return err
			}
		}
		if !ok || val == nil {
			continue
		}

		if err := validateFieldType(rule, val); err != nil {
			return err
		}
	}
	return nil
}

func validateFieldType(rule models.Rule, val any) error {
	switch strings.ToLower(rule.Type) {
	case "number":
		return checkNumericRule(rule, val)
	case "string":
		return checkStringRule(rule.Field, val)
	default:
		return nil
	}
}

