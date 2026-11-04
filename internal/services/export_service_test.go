package services_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func TestExportServiceGetRecordsAndCleanup(t *testing.T) {
	dir := t.TempDir()
	exportDir := filepath.Join(dir, "exports")
	_ = os.MkdirAll(exportDir, 0o755)

	outsideDir := filepath.Join(dir, "outside")
	_ = os.MkdirAll(outsideDir, 0o755)

	cfg := config.DefaultConfig()
	cfg.ExportDir = exportDir
	store := storage.NewMemoryRepository()

	svc := services.NewExportService(cfg, store)
	ctx := context.Background()

	// 1. GetRecords
	records := []models.Record{{"id": 1.0, "val": "A"}, {"id": 2.0, "val": "B"}}
	_ = store.SaveRecords(ctx, "job-exp", records)

	recs, total, err := svc.GetRecords(ctx, "job-exp", 0, 10)
	if err != nil || total != 2 || len(recs) != 2 {
		t.Fatalf("unexpected GetRecords: %v, total: %d, len: %d", err, total, len(recs))
	}

	// 2. CleanupJobExports
	fileInside := filepath.Join(exportDir, "artifact.json")
	_ = os.WriteFile(fileInside, []byte(`{"a":1}`), 0o600)

	fileOutside := filepath.Join(outsideDir, "important.txt")
	_ = os.WriteFile(fileOutside, []byte(`safe`), 0o600)

	job := &models.Job{
		ID:        "job-exp",
		Artifacts: []string{fileInside, fileOutside},
	}

	svc.CleanupJobExports(job)

	// fileInside must be deleted
	if _, err := os.Stat(fileInside); !os.IsNotExist(err) {
		t.Errorf("expected inside artifact to be deleted")
	}

	// fileOutside must NOT be deleted
	if _, err := os.Stat(fileOutside); os.IsNotExist(err) {
		t.Errorf("expected outside file to remain untouched")
	}
}
