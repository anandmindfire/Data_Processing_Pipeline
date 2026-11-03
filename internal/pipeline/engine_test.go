package pipeline

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/domain"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func TestValidateAndTransform(t *testing.T) {
	min := 0.0
	r := domain.Record{"name": " Alice ", "salary": "125"}
	rules := []domain.Rule{
		{Field: "name", Required: true, Type: "string", Normalize: "lower"},
		{Field: "salary", Required: true, Type: "number", Min: &min, Multiply: 2},
	}
	if err := validate(r, rules); err != nil {
		t.Fatal(err)
	}
	got, err := transform(r, rules)
	if err != nil {
		t.Fatal(err)
	}
	if got["name"] != "alice" || got["salary"] != 250.0 {
		t.Fatalf("unexpected transformed record: %#v", got)
	}
}

func TestValidationRules(t *testing.T) {
	min := 10.0
	max := 100.0

	// 1. Required field missing
	err := validate(domain.Record{"email": ""}, []domain.Rule{
		{Field: "email", Required: true},
	})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}

	// 2. Value below min
	err = validate(domain.Record{"val": 5.0}, []domain.Rule{
		{Field: "val", Type: "number", Min: &min},
	})
	if err == nil {
		t.Fatal("expected error for val < min")
	}

	// 3. Value above max
	err = validate(domain.Record{"val": 150.0}, []domain.Rule{
		{Field: "val", Type: "number", Max: &max},
	})
	if err == nil {
		t.Fatal("expected error for val > max")
	}

	// 4. Value not string
	err = validate(domain.Record{"name": 123}, []domain.Rule{
		{Field: "name", Type: "string"},
	})
	if err == nil {
		t.Fatal("expected error for non-string")
	}

	// 5. Value valid
	err = validate(domain.Record{"name": "Bob", "val": 50.0}, []domain.Rule{
		{Field: "name", Type: "string", Required: true},
		{Field: "val", Type: "number", Min: &min, Max: &max},
	})
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestTransformNormalizations(t *testing.T) {
	r := domain.Record{
		"code": " hello ",
		"tag":  " World ",
	}
	rules := []domain.Rule{
		{Field: "code", Normalize: "upper"},
		{Field: "tag", Normalize: "trim"},
	}
	got, err := transform(r, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["code"] != "HELLO" {
		t.Fatalf("expected HELLO, got %v", got["code"])
	}
	if got["tag"] != "World" {
		t.Fatalf("expected World, got %v", got["tag"])
	}
}

func TestValidateRejectsInvalidNumber(t *testing.T) {
	if err := validate(domain.Record{"salary": "bad"}, []domain.Rule{{Field: "salary", Required: true, Type: "number"}}); err == nil {
		t.Fatal("expected numeric validation error")
	}
}

func TestPipelineEndToEndWithCSVAndJSON(t *testing.T) {
	dir := t.TempDir()
	csvInput := filepath.Join(dir, "in.csv")
	if err := os.WriteFile(csvInput, []byte("team,amount\nA,10\nA,20\nB,5\nB,bad\n"), 0644); err != nil {
		t.Fatal(err)
	}
	jsonInput := filepath.Join(dir, "in.json")
	if err := os.WriteFile(jsonInput, []byte(`[{"team":"A","amount":30},{"team":"B","amount":15}]`), 0644); err != nil {
		t.Fatal(err)
	}

	store, err := storage.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	outJSON := filepath.Join(dir, "out.json")
	outCSV := filepath.Join(dir, "out.csv")

	j := &domain.Job{
		ID:     "multi-source-job",
		Status: domain.Pending,
		Config: domain.Config{
			Sources: []domain.Source{
				{Type: "csv", Name: "csv-source", Path: csvInput},
				{Type: "json", Name: "json-source", Path: jsonInput},
			},
			Rules: []domain.Rule{
				{Field: "team", Type: "string", Required: true, Normalize: "upper"},
				{Field: "amount", Type: "number", Required: true},
			},
			Aggregations: []domain.Aggregation{
				{GroupBy: "team", Field: "amount", Operation: "sum", Name: "team_sum"},
				{GroupBy: "team", Field: "amount", Operation: "avg", Name: "team_avg"},
				{Field: "amount", Operation: "sum", Name: "total_sum"}, // global aggregation
				{Field: "amount", Operation: "count", Name: "total_count"},
			},
			Exports: []domain.ExportTarget{
				{Type: "json", Path: outJSON},
				{Type: "csv", Path: outCSV},
			},
			ValidationWorkers: 2,
			TransformWorkers:  2,
			BufferSize:        16,
		},
		CreatedAt: time.Now(),
	}

	if err = store.PutJob(j); err != nil {
		t.Fatal(err)
	}
	engine := Engine{Store: store}
	engine.Run(context.Background(), j, func(*domain.Job) {})

	saved, ok := store.GetJob(j.ID)
	if !ok {
		t.Fatal("job missing")
	}
	if saved.Status != domain.Completed {
		t.Fatalf("status=%s", saved.Status)
	}

	// 4 from CSV + 2 from JSON = 6 discovered
	// 5 valid records, 1 rejected ("bad" number in CSV)
	if saved.Metrics.Discovered != 6 || saved.Metrics.Processed != 5 || saved.Metrics.Rejected != 1 {
		t.Fatalf("unexpected metrics: %+v", saved.Metrics)
	}

	// Check team sums:
	// A: 10 + 20 + 30 = 60
	// B: 5 + 15 = 20
	teamSums := saved.Aggregates["team_sum"].(map[string]any)
	if teamSums["A"].(float64) != 60 || teamSums["B"].(float64) != 20 {
		t.Fatalf("unexpected team_sum aggregates: %v", teamSums)
	}

	// Check team averages:
	// A: 60 / 3 = 20
	// B: 20 / 2 = 10
	teamAvgs := saved.Aggregates["team_avg"].(map[string]any)
	if teamAvgs["A"].(float64) != 20 || teamAvgs["B"].(float64) != 10 {
		t.Fatalf("unexpected team_avg aggregates: %v", teamAvgs)
	}

	// Check global sum: 60 + 20 = 80
	totalSum := saved.Aggregates["total_sum"].(map[string]any)
	if totalSum["_all"].(float64) != 80 {
		t.Fatalf("unexpected total_sum: %v", totalSum)
	}

	// Check total count: 5
	totalCount := saved.Aggregates["total_count"].(map[string]any)
	if totalCount["_all"].(float64) != 5 {
		t.Fatalf("unexpected total_count: %v", totalCount)
	}

	// Verify exported CSV
	cf, err := os.Open(outCSV)
	if err != nil {
		t.Fatalf("failed to open exported CSV: %v", err)
	}
	defer cf.Close()
	rows, err := csv.NewReader(cf).ReadAll()
	if err != nil {
		t.Fatalf("failed to read exported CSV: %v", err)
	}
	// 1 header + 5 rows = 6 rows
	if len(rows) != 6 {
		t.Fatalf("expected 6 rows in exported CSV, got %d", len(rows))
	}

	// Verify error collection
	errs := store.Errors(j.ID)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].Source != "csv-source" || errs[0].Stage != "validation" {
		t.Fatalf("unexpected error payload: %+v", errs[0])
	}
}

func TestPipelineCancellation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "in.csv")
	_ = os.WriteFile(input, []byte("x\n1\n2\n3\n"), 0644)
	store, _ := storage.New(filepath.Join(dir, "state"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := &domain.Job{
		ID: "cancel",
		Config: domain.Config{
			Sources: []domain.Source{{Type: "csv", Name: "x", Path: input}},
			Exports: []domain.ExportTarget{{Type: "json", Path: filepath.Join(dir, "out.json")}},
		},
	}
	_ = store.PutJob(j)
	engine := Engine{Store: store}
	engine.Run(ctx, j, func(*domain.Job) {})
	if j.Status != domain.Cancelled {
		t.Fatalf("expected cancelled, got %s", j.Status)
	}
}
