package transformation

import (
	"testing"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

func TestRecordTransformerTable(t *testing.T) {
	tests := []struct {
		name          string
		record        models.Record
		rules         []models.Rule
		expectedField string
		expectedVal   any
		expectErr     bool
	}{
		{
			name:   "String lower normalization",
			record: models.Record{"tag": " ENGINEERING "},
			rules: []models.Rule{
				{Field: "tag", Normalize: "lower"},
			},
			expectedField: "tag",
			expectedVal:   "engineering",
			expectErr:     false,
		},
		{
			name:   "String upper normalization",
			record: models.Record{"code": " alpha "},
			rules: []models.Rule{
				{Field: "code", Normalize: "upper"},
			},
			expectedField: "code",
			expectedVal:   "ALPHA",
			expectErr:     false,
		},
		{
			name:   "String trim normalization",
			record: models.Record{"city": "  Seattle  "},
			rules: []models.Rule{
				{Field: "city", Normalize: "trim"},
			},
			expectedField: "city",
			expectedVal:   "Seattle",
			expectErr:     false,
		},
		{
			name:   "Numeric multiplication",
			record: models.Record{"price": "100"},
			rules: []models.Rule{
				{Field: "price", Type: "number", Multiply: 1.25},
			},
			expectedField: "price",
			expectedVal:   125.0,
			expectErr:     false,
		},
		{
			name:   "Invalid number multiplication returns error",
			record: models.Record{"price": "not_a_number"},
			rules: []models.Rule{
				{Field: "price", Type: "number", Multiply: 2.0},
			},
			expectErr: true,
		},
	}

	transformer := NewTransformer()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := transformer.Transform(tc.record, tc.rules)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if !tc.expectErr {
				if got[tc.expectedField] != tc.expectedVal {
					t.Fatalf("expected field %s to be %v, got: %v", tc.expectedField, tc.expectedVal, got[tc.expectedField])
				}
			}
		})
	}
}

