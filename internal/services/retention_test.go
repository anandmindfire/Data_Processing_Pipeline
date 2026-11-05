package services_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func TestPurgeExpiredRemovesOnlyOldFinishedJobs(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ExportDir = t.TempDir()
	store := storage.NewMemoryRepository()
	svc := services.NewPipelineService(cfg, store, nil)
	ctx := context.Background()

	oldArtifact := filepath.Join(cfg.ExportDir, "old.json")
	_ = os.WriteFile(oldArtifact, []byte("[]"), 0o644)
	now := time.Now()
	jobs := []*models.Job{
		{ID: "old-done", Status: models.StatusCompleted, CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-48 * time.Hour), Artifacts: []string{oldArtifact}},
		{ID: "old-failed", Status: models.StatusFailed, CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-48 * time.Hour)},
		{ID: "old-running", Status: models.StatusRunning, CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-48 * time.Hour)},
		{ID: "fresh", Status: models.StatusCompleted, CreatedAt: now, UpdatedAt: now},
	}
	for _, j := range jobs {
		_ = store.SaveJob(ctx, j)
	}

	n, err := svc.PurgeExpired(ctx, now.Add(-24*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("expected 2 purged, got %d (%v)", n, err)
	}
	for id, want := range map[string]bool{"old-done": false, "old-failed": false, "old-running": true, "fresh": true} {
		if _, found, _ := store.GetJob(ctx, id); found != want {
			t.Fatalf("%s: found=%v want %v", id, found, want)
		}
	}
	if _, err := os.Stat(oldArtifact); !os.IsNotExist(err) {
		t.Fatal("expected purged job's artifacts to be removed")
	}
}
