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

// slowReader emits records with a delay and then blocks until released, so a test can observe
// the job while it is still RUNNING.
type slowReader struct {
	n       int
	release chan struct{}
}

func (r *slowReader) Read(ctx context.Context, s models.Source, _ int, out chan<- ingestion.Item, onDiscovered func()) error {
	for i := 0; i < r.n; i++ {
		select {
		case out <- ingestion.Item{Source: s.Name, Index: i + 1, Record: models.Record{"v": float64(i)}}:
			onDiscovered()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-r.release:
	case <-ctx.Done():
	}
	return nil
}

func TestLiveProgressIsPersistedWhileRunning(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewMemoryRepository()
	reader := &slowReader{n: 50, release: make(chan struct{})}
	pipe := NewPipeline(reader, validation.NewValidator(), transformation.NewTransformer(),
		export.NewExporter(dir), store, metrics.NewCollector(),
		WithProgressInterval(10*time.Millisecond), WithFlushInterval(5*time.Millisecond))

	job := &models.Job{ID: "live", CreatedAt: time.Now(), Config: models.Config{
		Sources: []models.Source{{Type: "json", Name: "slow"}},
		Exports: []models.ExportTarget{{Type: "json"}},
	}}
	done := make(chan struct{})
	go func() {
		pipe.Execute(context.Background(), job, nil)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for {
		got, _, _ := store.GetJob(context.Background(), "live")
		if got != nil && got.Status == models.StatusRunning && got.Metrics.Processed == 50 && got.Metrics.Discovered == 50 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("live progress never reached storage; last snapshot: %+v", got)
		case <-time.After(5 * time.Millisecond):
		}
	}

	close(reader.release)
	<-done
	got, _, _ := store.GetJob(context.Background(), "live")
	if got.Status != models.StatusCompleted || got.Metrics.Processed != 50 || got.Metrics.EndedAt == nil {
		t.Fatalf("unexpected final job state: %+v", got)
	}
}
