package stages

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

func TestStagesValidation(t *testing.T) {
	min := 10.0
	max := 100.0
	rules := []models.Rule{
		{Field: "username", Type: "string", Required: true},
		{Field: "score", Type: "number", Required: true, Min: &min, Max: &max},
	}

	// Valid
	err := ValidateRecord(models.Record{"username": "carol", "score": 85.0}, rules)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	// Missing required field
	err = ValidateRecord(models.Record{"score": 85.0}, rules)
	if err == nil {
		t.Fatal("expected error for missing required field")
	}

	// Below min
	err = ValidateRecord(models.Record{"username": "carol", "score": 5.0}, rules)
	if err == nil {
		t.Fatal("expected error for score < min")
	}

	// Non numeric
	err = ValidateRecord(models.Record{"username": "carol", "score": "abc"}, rules)
	if err == nil {
		t.Fatal("expected error for non-numeric score")
	}
}

func TestStagesTransformation(t *testing.T) {
	rules := []models.Rule{
		{Field: "name", Normalize: "lower"},
		{Field: "tag", Normalize: "upper"},
		{Field: "dept", Normalize: "trim"},
		{Field: "amount", Type: "number", Multiply: 1.5},
	}

	in := models.Record{
		"name":   " ALICE ",
		"tag":    " engineering ",
		"dept":   "  ops  ",
		"amount": "100",
	}

	out, err := TransformRecord(in, rules)
	if err != nil {
		t.Fatalf("TransformRecord failed: %v", err)
	}

	if out["name"] != "alice" {
		t.Fatalf("expected alice, got %v", out["name"])
	}
	if out["tag"] != "ENGINEERING" {
		t.Fatalf("expected ENGINEERING, got %v", out["tag"])
	}
	if out["dept"] != "ops" {
		t.Fatalf("expected ops, got %v", out["dept"])
	}
	if out["amount"] != 150.0 {
		t.Fatalf("expected 150, got %v", out["amount"])
	}
}

func TestStagesAggregation(t *testing.T) {
	defs := []models.Aggregation{
		{GroupBy: "city", Field: "sales", Operation: "sum", Name: "city_sales"},
		{GroupBy: "city", Field: "sales", Operation: "avg", Name: "city_avg"},
		{Field: "sales", Operation: "count", Name: "total_count"},
	}

	agg := NewAggregator(defs)
	agg.Process(models.Record{"city": "NYC", "sales": 100.0})
	agg.Process(models.Record{"city": "NYC", "sales": 200.0})
	agg.Process(models.Record{"city": "SF", "sales": 300.0})

	results := agg.Finalize()

	citySales := results["city_sales"].(map[string]float64)
	if citySales["NYC"] != 300.0 || citySales["SF"] != 300.0 {
		t.Fatalf("unexpected city_sales: %v", citySales)
	}

	cityAvg := results["city_avg"].(map[string]float64)
	if cityAvg["NYC"] != 150.0 || cityAvg["SF"] != 300.0 {
		t.Fatalf("unexpected city_avg: %v", cityAvg)
	}

	totalCount := results["total_count"].(map[string]float64)
	if totalCount["_all"] != 3.0 {
		t.Fatalf("unexpected total_count: %v", totalCount)
	}
}

func TestStagesExport(t *testing.T) {
	dir := t.TempDir()
	records := []models.Record{
		{"id": 1, "name": "Alpha"},
		{"id": 2, "name": "Beta"},
	}

	targets := []models.ExportTarget{
		{Type: "json", Path: filepath.Join(dir, "out.json")},
		{Type: "csv", Path: filepath.Join(dir, "out.csv")},
	}

	primaryPath, err := Export(records, targets, dir, "test-export")
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	if primaryPath != targets[0].Path {
		t.Fatalf("expected primary path %s, got %s", targets[0].Path, primaryPath)
	}

	if _, err := os.Stat(targets[0].Path); err != nil {
		t.Fatalf("JSON export missing: %v", err)
	}
	if _, err := os.Stat(targets[1].Path); err != nil {
		t.Fatalf("CSV export missing: %v", err)
	}
}

