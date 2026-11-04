package transformation

import (
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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
			name:   "String title normalization",
			record: models.Record{"city": "new york city"},
			rules: []models.Rule{
				{Field: "city", Normalize: "title"},
			},
			expectedField: "city",
			expectedVal:   "New York City",
			expectErr:     false,
		},
		{
			name:   "Numeric multiplication and addition",
			record: models.Record{"price": "100"},
			rules: []models.Rule{
				{Field: "price", Type: "number", Multiply: 1.25, Add: 5.0},
			},
			expectedField: "price",
			expectedVal:   130.0,
			expectErr:     false,
		},
		{
			name:   "Default value fallback when missing",
			record: models.Record{"name": "Alice"},
			rules: []models.Rule{
				{Field: "status", Default: "active"},
			},
			expectedField: "status",
			expectedVal:   "active",
			expectErr:     false,
		},
		{
			name:   "Invalid number multiplication returns error",
			record: models.Record{"price": "not_a_number"},
			rules: []models.Rule{
				{Field: "price", Type: "number", Multiply: 2.0},
			},
			expectedField: "price",
			expectedVal:   nil,
			expectErr:     true,
		},
	}

	transformer := NewTransformer()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := transformer.Transform(tc.record, tc.rules)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if !tc.expectErr && res[tc.expectedField] != tc.expectedVal {
				t.Fatalf("expected %v, got %v", tc.expectedVal, res[tc.expectedField])
			}
		})
	}
}

func TestTransformerImmutability(t *testing.T) {
	transformer := NewTransformer()
	original := models.Record{"city": "  Denver  ", "count": "10"}
	rules := []models.Rule{
		{Field: "city", Normalize: "trim"},
		{Field: "count", Type: "number", Multiply: 2.0},
	}

	transformed, err := transformer.Transform(original, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if original["city"] != "  Denver  " {
		t.Fatalf("original record was mutated: city = %v", original["city"])
	}
	if original["count"] != "10" {
		t.Fatalf("original record was mutated: count = %v", original["count"])
	}
	if transformed["city"] != "Denver" {
		t.Fatalf("transformed city mismatch: got %v", transformed["city"])
	}
	if transformed["count"] != 20.0 {
		t.Fatalf("transformed count mismatch: got %v", transformed["count"])
	}
}
