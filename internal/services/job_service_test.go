package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

type mockRunner struct {
	executed bool
}

func (m *mockRunner) Execute(_ context.Context, job *models.Job, update func(*models.Job)) {
	m.executed = true
	job.Status = models.StatusCompleted
	if update != nil {
		update(job)
	}
}

func TestJobServiceLifecycle(t *testing.T) {
	cfg := &configs.Config{MaxActiveJobs: 2, ExportDir: t.TempDir()}
	store := storage.NewMemoryRepository()
	runner := &mockRunner{}
	service := services.NewJobService(cfg, store, runner)

	ctx := context.Background()
	jobCfg := models.Config{
		Sources: []models.Source{{Type: "csv", Name: "test"}},
		Exports: []models.ExportTarget{{Type: "json"}},
	}

	// 1. Submit job
	job, err := service.SubmitJob(ctx, jobCfg)
	if err != nil {
		t.Fatalf("SubmitJob failed: %v", err)
	}
	if job.ID == "" {
		t.Fatal("expected non-empty job ID")
	}

	// Wait briefly for background execution
	time.Sleep(20 * time.Millisecond)

	// 2. Get job
	got, found, err := service.GetJob(ctx, job.ID)
	if err != nil || !found || got.ID != job.ID {
		t.Fatalf("GetJob failed: found=%v, err=%v", found, err)
	}

	// 3. List jobs
	list, err := service.ListJobs(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListJobs failed: count=%d, err=%v", len(list), err)
	}

	// 4. Delete job
	if err := service.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	_, found, _ = service.GetJob(ctx, job.ID)
	if found {
		t.Fatal("expected job to be deleted")
	}
}
