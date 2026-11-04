package services

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

type dummyRunner struct{}

func (d *dummyRunner) Execute(ctx context.Context, job *models.Job, onUpdate func(*models.Job)) {
	job.Status = models.StatusCompleted
	if onUpdate != nil {
		onUpdate(job)
	}
}

func TestPipelineServiceLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		DataDir:         filepath.Join(tempDir, "data"),
		ExportDir:       filepath.Join(tempDir, "exports"),
		MaxActiveJobs:   2,
		ShutdownTimeout: 5 * time.Second,
	}

	store := storage.NewMemoryRepository()
	defer func() { _ = store.Close() }()

	svc := NewPipelineService(cfg, store, &dummyRunner{})

	ctx := context.Background()

	// 1. SubmitJob
	jobCfg := models.Config{
		Sources: []models.Source{{Type: "csv", Path: "test.csv"}},
	}
	job, err := svc.SubmitJob(ctx, jobCfg)
	if err != nil {
		t.Fatalf("SubmitJob failed: %v", err)
	}
	if job == nil || job.ID == "" {
		t.Fatalf("expected valid job, got %+v", job)
	}

	// 2. GetJob
	retrieved, found, err := svc.GetJob(ctx, job.ID)
	if err != nil || !found {
		t.Fatalf("GetJob failed: found=%v, err=%v", found, err)
	}
	if retrieved.ID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, retrieved.ID)
	}

	// 3. ListJobs
	jobs, total, err := svc.ListJobs(ctx, storage.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if total < 1 || len(jobs) < 1 {
		t.Errorf("expected at least 1 job, got len=%d total=%d", len(jobs), total)
	}

	// 4. GetProgress
	prog, progFound, err := svc.GetProgress(ctx, job.ID)
	if err != nil || !progFound {
		t.Fatalf("GetProgress failed: found=%v, err=%v", progFound, err)
	}
	if prog.JobID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, prog.JobID)
	}

	// 5. GetResults
	res, resFound, err := svc.GetResults(ctx, job.ID, 0, 10)
	if err != nil || !resFound {
		t.Fatalf("GetResults failed: found=%v, err=%v", resFound, err)
	}
	if res.JobID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, res.JobID)
	}

	// 6. GetErrors
	errs, errCount, errFound, err := svc.GetErrors(ctx, job.ID, 0, 10)
	if err != nil || !errFound {
		t.Fatalf("GetErrors failed: found=%v, err=%v", errFound, err)
	}
	if errCount != 0 || len(errs) != 0 {
		t.Errorf("expected 0 errors, got %d", errCount)
	}

	// 7. CancelJob
	_ = svc.CancelJob(job.ID)

	// Wait briefly for job to reach terminal state before deletion
	for i := 0; i < 20; i++ {
		j, _, _ := svc.GetJob(ctx, job.ID)
		if j != nil && (j.Status == models.StatusCancelled || j.Status == models.StatusFailed || j.Status == models.StatusCompleted) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 8. DeleteJob
	if err := svc.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// Confirm deleted
	_, foundAfterDelete, _ := svc.GetJob(ctx, job.ID)
	if foundAfterDelete {
		t.Errorf("expected job to be deleted")
	}

	// 9. Shutdown
	if err := svc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestPipelineServiceNotFound(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		DataDir:   filepath.Join(tempDir, "data"),
		ExportDir: filepath.Join(tempDir, "exports"),
	}
	store := storage.NewMemoryRepository()
	defer func() { _ = store.Close() }()

	svc := NewPipelineService(cfg, store, &dummyRunner{})
	ctx := context.Background()

	_, found, err := svc.GetResults(ctx, "nonexistent", 0, 10)
	if err != nil || found {
		t.Errorf("expected found=false for nonexistent results, got found=%v err=%v", found, err)
	}

	err = svc.DeleteJob(ctx, "nonexistent")
	if err == nil {
		t.Errorf("expected error deleting nonexistent job")
	}
}
