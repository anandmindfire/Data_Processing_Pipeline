package validation

import (
	"regexp"
	"sync"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Validator defines the behavior for validating a record against rules.
type Validator interface {
	Validate(record models.Record, rules []models.Rule) error
}

// RecordValidator is the concrete implementation of Validator.
type RecordValidator struct {
	regexCache sync.Map
}

// NewValidator constructs a new RecordValidator.
func NewValidator() *RecordValidator {
	return &RecordValidator{}
}

// getOrCompileRegex returns a thread-safe cached compiled regexp pattern.
func (v *RecordValidator) getOrCompileRegex(pattern string) (*regexp.Regexp, error) {
	if cached, ok := v.regexCache.Load(pattern); ok {
		if re, ok := cached.(*regexp.Regexp); ok {
			return re, nil
		}
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	v.regexCache.Store(pattern, re)
	return re, nil
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

		if err := v.validateFieldType(rule, val); err != nil {
			return err
		}
	}
	return nil
}
