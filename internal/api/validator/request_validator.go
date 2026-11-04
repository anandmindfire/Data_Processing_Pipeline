package validator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// RequestValidator performs structural and semantic validation on pipeline job requests.
type RequestValidator struct{}

// NewRequestValidator constructs a new RequestValidator.
func NewRequestValidator() *RequestValidator {
	return &RequestValidator{}
}

// Validate verifies that the job configuration meets all validation constraints.
func (v *RequestValidator) Validate(cfg *models.Config) error {
	if cfg == nil {
		return errors.New("request configuration is required")
	}
	if err := validateSources(cfg.Sources); err != nil {
		return err
	}
	if err := validateExports(cfg.Exports); err != nil {
		return err
	}
	if err := validateRules(cfg.Rules); err != nil {
		return err
	}
	return validateAggregations(cfg.Aggregations)
}

// ValidateJobConfig is a package-level helper validating a Config.
func ValidateJobConfig(cfg *models.Config) error {
	return NewRequestValidator().Validate(cfg)
}

func validateSources(sources []models.Source) error {
	if len(sources) == 0 {
		return errors.New("at least one source is required")
	}
	for i, s := range sources {
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("source[%d]: name is required", i)
		}
		if s.Path == "" && s.URL == "" {
			return fmt.Errorf("source[%d]: either path or url is required", i)
		}
	}
	return nil
}

func validateExports(exports []models.ExportTarget) error {
	if len(exports) == 0 {
		return errors.New("at least one export destination is required")
	}
	for i, e := range exports {
		if strings.TrimSpace(e.Type) == "" {
			return fmt.Errorf("export[%d]: type is required", i)
		}
	}
	return nil
}

func validateRules(rules []models.Rule) error {
	for i, r := range rules {
		if strings.TrimSpace(r.Field) == "" {
			return fmt.Errorf("rule[%d]: field is required", i)
		}
		if r.Min != nil && r.Max != nil && *r.Min > *r.Max {
			return fmt.Errorf("rule[%d]: min cannot be greater than max", i)
		}
	}
	return nil
}

func validateAggregations(aggs []models.Aggregation) error {
	for i, a := range aggs {
		if strings.TrimSpace(a.Field) == "" {
			return fmt.Errorf("aggregation[%d]: field is required", i)
		}
		if strings.TrimSpace(a.Operation) == "" {
			return fmt.Errorf("aggregation[%d]: operation is required", i)
		}
	}
	return nil
}
