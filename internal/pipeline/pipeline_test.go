package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

func TestPipelineDirectExecution(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	_ = os.WriteFile(csvPath, []byte("dept,salary\nEng,100\nEng,200\n"), 0644)

	store := storage.NewMemoryRepository()
	pipe := NewPipeline(
		ingestion.NewReader(nil),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(dir),
		store,
		metrics.NewCollector(),
	)

	job := &models.Job{
		ID:        "direct-job",
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
		Config: models.Config{
			Sources: []models.Source{{Type: "csv", Name: "test-src", Path: csvPath}},
			Rules: []models.Rule{
				{Field: "dept", Type: "string", Required: true},
				{Field: "salary", Type: "number", Required: true},
			},
			Aggregations: []models.Aggregation{
				{GroupBy: "dept", Field: "salary", Operation: "sum", Name: "dept_sum"},
			},
			Exports: []models.ExportTarget{
				{Type: "json", Path: filepath.Join(dir, "out.json")},
			},
			ValidationWorkers: 2,
			TransformWorkers:  2,
			BufferSize:        10,
		},
	}

	pipe.Execute(context.Background(), job, nil)

	if job.Status != models.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %s", job.Status)
	}
	if job.Metrics.Processed != 2 {
		t.Fatalf("expected 2 processed records, got %d", job.Metrics.Processed)
	}
	sums, ok := job.Aggregates["dept_sum"].(map[string]float64)
	if !ok || sums["Eng"] != 300.0 {
		t.Fatalf("expected Eng sum 300, got %v", job.Aggregates["dept_sum"])
	}
}
