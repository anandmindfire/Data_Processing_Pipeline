package services_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
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

var spec = models.JobSpec{
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
	if job.Spec.ValidationWorkers == 0 || job.Spec.TimeoutSeconds == 0 {
		t.Fatalf("expected service defaults applied, got %+v", job.Spec)
	}
	<-runner.started

	if err := svc.DeleteJob(ctx, job.ID); !errors.Is(err, services.ErrJobStillRunning) {
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
	if _, err := svc.SubmitJob(ctx, spec); !errors.Is(err, services.ErrCapacityReached) {
		t.Fatalf("expected ErrCapacityReached, got %v", err)
	}

	if err := svc.CancelJob(ctx, job.ID); err != nil {
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
	if _, err := svc.SubmitJob(ctx, spec); !errors.Is(err, services.ErrShuttingDown) {
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

// TestReplicasShareOneStore runs two JobServices ("replicas") on one store: a cancel received by
// replica B stops a job running on replica A, and once A stops heartbeating, B fails A's jobs.
func TestReplicasShareOneStore(t *testing.T) {
	store := storage.NewMemoryRepository()
	newReplica := func(id string) (*services.JobService, *blockingRunner) {
		cfg := config.DefaultConfig()
		cfg.InstanceID = id
		cfg.JobLease = 300 * time.Millisecond
		runner := &blockingRunner{store: store, started: make(chan string, 8), release: make(chan struct{})}
		return services.NewJobService(cfg, store, runner), runner
	}
	a, runnerA := newReplica("replica-a")
	b, _ := newReplica("replica-b")

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	ctxA, stopA := context.WithCancel(ctx)
	go a.RunCoordinator(ctxA)
	go b.RunCoordinator(ctx)

	// 1. Cross-replica cancellation.
	job, err := a.SubmitJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	<-runnerA.started
	if job.Owner != "replica-a" {
		t.Fatalf("expected owner replica-a, got %q", job.Owner)
	}
	if err := b.DeleteJob(ctx, job.ID); !errors.Is(err, services.ErrJobStillRunning) {
		t.Fatalf("replica B must refuse to delete a job running on A, got %v", err)
	}
	if err := b.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("cancel via replica B: %v", err)
	}
	waitFor(t, func() bool {
		j, _, _ := store.GetJob(ctx, job.ID)
		return j.Status == models.StatusCancelled
	})
	if err := b.CancelJob(ctx, job.ID); !errors.Is(err, services.ErrJobNotRunning) {
		t.Fatalf("cancelling a finished job: expected ErrJobNotRunning, got %v", err)
	}
	if err := b.CancelJob(ctx, "missing"); !errors.Is(err, services.ErrJobNotFound) {
		t.Fatalf("cancelling a missing job: expected ErrJobNotFound, got %v", err)
	}

	// 2. Replica A goes silent (coordinator stopped, as if the node died): B fails its job.
	job2, err := a.SubmitJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	<-runnerA.started
	time.Sleep(100 * time.Millisecond)
	if j, _, _ := store.GetJob(ctx, job2.ID); j.Status.IsTerminal() {
		t.Fatalf("a heartbeating job must not be reclaimed, got %s", j.Status)
	}
	stopA()
	waitFor(t, func() bool {
		j, _, _ := store.GetJob(ctx, job2.ID)
		return j.Status == models.StatusFailed && strings.Contains(j.Error, "interrupted")
	})

	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = a.Shutdown(sctx)
}
