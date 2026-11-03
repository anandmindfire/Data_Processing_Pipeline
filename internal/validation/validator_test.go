package validation

import (
	"testing"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

func TestRecordValidatorTable(t *testing.T) {
	minVal := 10.0
	maxVal := 100.0

	tests := []struct {
		name      string
		record    models.Record
		rules     []models.Rule
		expectErr bool
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
		},
		{
			name:   "Value below minimum",
			record: models.Record{"score": 5.0},
			rules: []models.Rule{
				{Field: "score", Type: "number", Min: &minVal},
			},
			expectErr: true,
		},
		{
			name:   "Value above maximum",
			record: models.Record{"score": 150.0},
			rules: []models.Rule{
				{Field: "score", Type: "number", Max: &maxVal},
			},
			expectErr: true,
		},
		{
			name:   "Non-numeric value for numeric rule",
			record: models.Record{"score": "invalid_number"},
			rules: []models.Rule{
				{Field: "score", Type: "number"},
			},
			expectErr: true,
		},
		{
			name:   "Non-string value for string rule",
			record: models.Record{"name": 12345},
			rules: []models.Rule{
				{Field: "name", Type: "string"},
			},
			expectErr: true,
		},
	}

	validator := NewValidator()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.record, tc.rules)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
		})
	}
}

