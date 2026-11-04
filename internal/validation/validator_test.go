package validation

import (
	"errors"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestRecordValidatorTable(t *testing.T) {
	minVal := 10.0
	maxVal := 100.0

	tests := []struct {
		name      string
		record    models.Record
		rules     []models.Rule
		expectErr bool
		targetErr error
	}{
		{
			name:   "Valid record with string and numeric range",
			record: models.Record{"name": "Alice", "score": 75.0},
			rules: []models.Rule{
				{Field: "name", Type: "string", Required: true},
				{Field: "score", Type: "number", Required: true, Min: &minVal, Max: &maxVal},
			},
			expectErr: false,
		},
		{
			name:   "Missing required field",
			record: models.Record{"score": 50.0},
			rules: []models.Rule{
				{Field: "name", Required: true},
			},
			expectErr: true,
			targetErr: ErrRequiredField,
		},
		{
			name:   "Value below minimum",
			record: models.Record{"score": 5.0},
			rules: []models.Rule{
				{Field: "score", Type: "number", Min: &minVal},
			},
			expectErr: true,
			targetErr: ErrNumericConstraint,
		},
		{
			name:   "Value above maximum",
			record: models.Record{"score": 150.0},
			rules: []models.Rule{
				{Field: "score", Type: "number", Max: &maxVal},
			},
			expectErr: true,
			targetErr: ErrNumericConstraint,
		},
		{
			name:   "Non-numeric value for numeric rule",
			record: models.Record{"score": "invalid_number"},
			rules: []models.Rule{
				{Field: "score", Type: "number"},
			},
			expectErr: true,
			targetErr: ErrTypeMismatch,
		},
		{
			name:   "Non-string value for string rule",
			record: models.Record{"name": 12345},
			rules: []models.Rule{
				{Field: "name", Type: "string"},
			},
			expectErr: true,
			targetErr: ErrTypeMismatch,
		},
		{
			name:   "Valid pattern matching",
			record: models.Record{"code": "EMP-101"},
			rules: []models.Rule{
				{Field: "code", Type: "string", Pattern: `^EMP-\d+$`},
			},
			expectErr: false,
		},
		{
			name:   "Pattern mismatch",
			record: models.Record{"code": "INVALID-CODE"},
			rules: []models.Rule{
				{Field: "code", Type: "string", Pattern: `^EMP-\d+$`},
			},
			expectErr: true,
			targetErr: ErrPatternMismatch,
		},
		{
			name:   "Valid boolean field",
			record: models.Record{"active": "true"},
			rules: []models.Rule{
				{Field: "active", Type: "bool"},
			},
			expectErr: false,
		},
		{
			name:   "Invalid boolean field",
			record: models.Record{"active": "not_a_bool"},
			rules: []models.Rule{
				{Field: "active", Type: "bool"},
			},
			expectErr: true,
			targetErr: ErrTypeMismatch,
		},
	}

	validator := NewValidator()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.record, tc.rules)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if tc.targetErr != nil && !errors.Is(err, tc.targetErr) {
				t.Fatalf("expected error to wrap %v, got %v", tc.targetErr, err)
			}
		})
	}
}
