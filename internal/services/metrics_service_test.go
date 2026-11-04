package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

func TestMetricsServiceGetProgressAndErrors(t *testing.T) {
	store := storage.NewMemoryRepository()
	svc := services.NewMetricsService(store)
	ctx := context.Background()

	// 1. Missing job returns found=false
	prog, found, err := svc.GetProgress(ctx, "nonexistent")
	if err != nil || found || prog != nil {
		t.Fatalf("expected not found for nonexistent job, got %v, %v, %v", err, found, prog)
	}

	errs, total, found, err := svc.GetErrors(ctx, "nonexistent", 0, 10)
	if err != nil || found || total != 0 || len(errs) != 0 {
		t.Fatalf("expected not found for nonexistent errors: %v, %v", err, found)
	}

	// 2. Existing running job progress calculation
	now := time.Now()
	runningJob := &models.Job{
		ID:        "job-running",
		Status:    models.StatusRunning,
		CreatedAt: now,
		Metrics: models.Metrics{
			Discovered: 100,
			Processed:  40,
			Rejected:   10,
			Errors:     10,
		},
	}
	_ = store.SaveJob(ctx, runningJob)
	_ = store.AddErrors(ctx, "job-running", []models.RecordError{
		{Stage: "transform", Message: "cannot parse int", At: now},
	})

	progRunning, found, err := svc.GetProgress(ctx, "job-running")
	if err != nil || !found {
		t.Fatalf("failed GetProgress: %v", err)
	}
	if progRunning.Percent != 50.0 {
		t.Errorf("expected 50%% progress, got %f", progRunning.Percent)
	}
	if progRunning.Pending != 50 {
		t.Errorf("expected 50 pending, got %d", progRunning.Pending)
	}

	// 3. Completed job percent is 100.0
	runningJob.Status = models.StatusCompleted
	_ = store.SaveJob(ctx, runningJob)
	progCompleted, _, _ := svc.GetProgress(ctx, "job-running")
	if progCompleted.Percent != 100.0 {
		t.Errorf("expected 100%% for completed job, got %f", progCompleted.Percent)
	}

	// 4. GetErrors
	retErrs, retTotal, found, err := svc.GetErrors(ctx, "job-running", 0, 10)
	if err != nil || !found || retTotal != 1 || len(retErrs) != 1 {
		t.Fatalf("unexpected GetErrors: %v, total: %d, len: %d", err, retTotal, len(retErrs))
	}
}
