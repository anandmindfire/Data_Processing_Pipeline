package pipeline

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func TestValidateAndTransform(t *testing.T) {
	min := 0.0
	r := models.Record{"name": " Alice ", "salary": "125"}
	rules := []models.Rule{
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
	err := validate(models.Record{"email": ""}, []models.Rule{
		{Field: "email", Required: true},
	})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}

	// 2. Value below min
	err = validate(models.Record{"val": 5.0}, []models.Rule{
		{Field: "val", Type: "number", Min: &min},
	})
	if err == nil {
		t.Fatal("expected error for val < min")
	}

	// 3. Value above max
	err = validate(models.Record{"val": 150.0}, []models.Rule{
		{Field: "val", Type: "number", Max: &max},
	})
	if err == nil {
		t.Fatal("expected error for val > max")
	}

	// 4. Value not string
	err = validate(models.Record{"name": 123}, []models.Rule{
		{Field: "name", Type: "string"},
	})
	if err == nil {
		t.Fatal("expected error for non-string")
	}

	// 5. Value valid
	err = validate(models.Record{"name": "Bob", "val": 50.0}, []models.Rule{
		{Field: "name", Type: "string", Required: true},
		{Field: "val", Type: "number", Min: &min, Max: &max},
	})
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestTransformNormalizations(t *testing.T) {
	r := models.Record{
		"code": " hello ",
		"tag":  " World ",
	}
	rules := []models.Rule{
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
	if err := validate(models.Record{"salary": "bad"}, []models.Rule{{Field: "salary", Required: true, Type: "number"}}); err == nil {
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

	j := &models.Job{
		ID:     "multi-source-job",
		Status: models.StatusPending,
		Config: models.Config{
			Sources: []models.Source{
				{Type: "csv", Name: "csv-source", Path: csvInput},
				{Type: "json", Name: "json-source", Path: jsonInput},
			},
			Rules: []models.Rule{
				{Field: "team", Type: "string", Required: true, Normalize: "upper"},
				{Field: "amount", Type: "number", Required: true},
			},
			Aggregations: []models.Aggregation{
				{GroupBy: "team", Field: "amount", Operation: "sum", Name: "team_sum"},
				{GroupBy: "team", Field: "amount", Operation: "avg", Name: "team_avg"},
				{Field: "amount", Operation: "sum", Name: "total_sum"}, // global aggregation
				{Field: "amount", Operation: "count", Name: "total_count"},
			},
			Exports: []models.ExportTarget{
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
	engine.Run(context.Background(), j, func(*models.Job) {})

	saved, ok := store.GetJob(j.ID)
	if !ok {
		t.Fatal("job missing")
	}
	if saved.Status != models.StatusCompleted {
		t.Fatalf("status=%s", saved.Status)
	}

	assertJobMetrics(t, saved.Metrics, 6, 5, 1)
	assertAggregates(t, saved.Aggregates)
	assertExportedCSV(t, outCSV, 6)
	assertCollectedErrors(t, store.Errors(j.ID))
}

func assertJobMetrics(t *testing.T, m models.Metrics, disc, proc, rej int64) {
	t.Helper()
	if m.Discovered != disc || m.Processed != proc || m.Rejected != rej {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func assertAggregates(t *testing.T, agg map[string]any) {
	t.Helper()
	teamSums, ok := agg["team_sum"].(map[string]any)
	if !ok {
		t.Fatalf("team_sum not a map: %T", agg["team_sum"])
	}
	sumA, _ := teamSums["A"].(float64)
	sumB, _ := teamSums["B"].(float64)
	if sumA != 60 || sumB != 20 {
		t.Fatalf("unexpected team_sum: A=%v, B=%v", sumA, sumB)
	}

	teamAvgs, ok := agg["team_avg"].(map[string]any)
	if !ok {
		t.Fatalf("team_avg not a map: %T", agg["team_avg"])
	}
	avgA, _ := teamAvgs["A"].(float64)
	avgB, _ := teamAvgs["B"].(float64)
	if avgA != 20 || avgB != 10 {
		t.Fatalf("unexpected team_avg: A=%v, B=%v", avgA, avgB)
	}

	totalSum, ok := agg["total_sum"].(map[string]any)
	if !ok {
		t.Fatalf("total_sum not a map: %T", agg["total_sum"])
	}
	if s, _ := totalSum["_all"].(float64); s != 80 {
		t.Fatalf("unexpected total_sum: %v", totalSum)
	}

	totalCount, ok := agg["total_count"].(map[string]any)
	if !ok {
		t.Fatalf("total_count not a map: %T", agg["total_count"])
	}
	if c, _ := totalCount["_all"].(float64); c != 5 {
		t.Fatalf("unexpected total_count: %v", totalCount)
	}
}

func assertExportedCSV(t *testing.T, path string, expectedRows int) {
	t.Helper()
	cf, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open exported CSV: %v", err)
	}
	defer cf.Close()

	rows, err := csv.NewReader(cf).ReadAll()
	if err != nil {
		t.Fatalf("failed to read exported CSV: %v", err)
	}
	if len(rows) != expectedRows {
		t.Fatalf("expected %d rows in exported CSV, got %d", expectedRows, len(rows))
	}
}

func assertCollectedErrors(t *testing.T, errs []models.RecordError) {
	t.Helper()
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
	j := &models.Job{
		ID: "cancel",
		Config: models.Config{
			Sources: []models.Source{{Type: "csv", Name: "x", Path: input}},
			Exports: []models.ExportTarget{{Type: "json", Path: filepath.Join(dir, "out.json")}},
		},
	}
	_ = store.PutJob(j)
	engine := Engine{Store: store}
	engine.Run(ctx, j, func(*models.Job) {})
	if j.Status != models.StatusCancelled {
		t.Fatalf("expected cancelled, got %s", j.Status)
	}
}
