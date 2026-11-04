package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	pkgerrors "github.com/anandmindfire/data-processing-pipeline/pkg/errors"
)

// blockingRunner runs until released or cancelled, then records a terminal state.
type blockingRunner struct {
	store   storage.Repository
	started chan string
	release chan struct{}
}

func (r *blockingRunner) Execute(ctx context.Context, job *models.Job, _ func(*models.Job)) {
	r.started <- job.ID
	select {
	case <-r.release:
		job.Status = models.StatusCompleted
	case <-ctx.Done():
		job.Status = models.StatusCancelled
		job.Error = context.Cause(ctx).Error()
	}
	_ = r.store.SaveJob(context.Background(), job)
}

func newService(t *testing.T, maxActive int) (*services.JobService, *blockingRunner, storage.Repository) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.MaxActiveJobs = maxActive
	store := storage.NewMemoryRepository()
	runner := &blockingRunner{store: store, started: make(chan string, 8), release: make(chan struct{})}
	return services.NewJobService(cfg, store, runner), runner, store
}

var spec = models.Config{
	Sources: []models.Source{{Type: "csv", Name: "test"}},
	Exports: []models.ExportTarget{{Type: "json"}},
}

func TestJobServiceLifecycle(t *testing.T) {
	svc, runner, _ := newService(t, 2)
	ctx := context.Background()

	job, err := svc.SubmitJob(ctx, spec)
	if err != nil || job.ID == "" {
		t.Fatalf("SubmitJob failed: %v", err)
	}
	if job.Config.ValidationWorkers == 0 || job.Config.TimeoutSeconds == 0 {
		t.Fatalf("expected service defaults applied, got %+v", job.Config)
	}
	<-runner.started

	if err := svc.DeleteJob(ctx, job.ID); !errors.Is(err, pkgerrors.ErrJobStillRunning) {
		t.Fatalf("expected ErrJobStillRunning, got %v", err)
	}
	close(runner.release)
	waitFor(t, func() bool { return !svc.IsRunning(job.ID) })

	list, total, err := svc.ListJobs(ctx, storage.ListOptions{})
	if err != nil || total != 1 || len(list) != 1 || list[0].Status != models.StatusCompleted {
		t.Fatalf("ListJobs: total=%d list=%+v err=%v", total, list, err)
	}
	if err := svc.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	if _, found, _ := svc.GetJob(ctx, job.ID); found {
		t.Fatal("expected job to be deleted")
	}
}

func TestJobServiceCapacityCancelAndShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)
	svc, runner, store := newService(t, 1)
	ctx := context.Background()

	job, err := svc.SubmitJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	<-runner.started
	if _, err := svc.SubmitJob(ctx, spec); !errors.Is(err, pkgerrors.ErrCapacityReached) {
		t.Fatalf("expected ErrCapacityReached, got %v", err)
	}

	if err := svc.CancelJob(job.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !svc.IsRunning(job.ID) })
	got, _, _ := store.GetJob(ctx, job.ID)
	if got.Status != models.StatusCancelled || got.Error != services.ErrCancelledByUser.Error() {
		t.Fatalf("expected user cancellation, got %s %q", got.Status, got.Error)
	}

	second, err := svc.SubmitJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	<-runner.started
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := svc.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	got, _, _ = store.GetJob(ctx, second.ID)
	if got.Status != models.StatusCancelled || got.Error != services.ErrServerShutdown.Error() {
		t.Fatalf("expected shutdown cancellation, got %s %q", got.Status, got.Error)
	}
	if _, err := svc.SubmitJob(ctx, spec); !errors.Is(err, pkgerrors.ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown after shutdown, got %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
