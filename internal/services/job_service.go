package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	pkgerrors "github.com/anandmindfire/data-processing-pipeline/pkg/errors"
)

// Cancellation causes recorded on the job (see models.Job.Error).
var (
	ErrCancelledByUser = errors.New("cancelled by user request")
	ErrServerShutdown  = errors.New("server shutting down")
)

// PipelineRunner defines the interface for executing a pipeline job.
type PipelineRunner interface {
	Execute(ctx context.Context, job *models.Job, onUpdate func(*models.Job))
}

// JobService manages job execution lifecycle, concurrency limits, and graceful shutdown.
type JobService struct {
	cfg           *config.Config
	storage       storage.Repository
	runner        PipelineRunner
	maxActiveJobs int

	mu       sync.Mutex
	cancels  map[string]context.CancelCauseFunc
	closing  bool
	inFlight sync.WaitGroup
}

// NewJobService creates a new JobService instance.
func NewJobService(cfg *config.Config, store storage.Repository, runner PipelineRunner) *JobService {
	maxActive := cfg.MaxActiveJobs
	if maxActive <= 0 {
		maxActive = 1
	}
	return &JobService{
		cfg:           cfg,
		storage:       store,
		runner:        runner,
		maxActiveJobs: maxActive,
		cancels:       make(map[string]context.CancelCauseFunc),
	}
}

// newJobID returns a sortable, collision-resistant ID: UTC timestamp + 48 random bits.
func newJobID(now time.Time) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return now.Format("20060102T150405.000Z") + "-" + hex.EncodeToString(b[:])
}

// applyServiceDefaults fills unset tuning knobs from the service configuration so the stored job
// shows the effective values it ran with.
func (s *JobService) applyServiceDefaults(cfg *models.Config) {
	if cfg.ValidationWorkers <= 0 {
		cfg.ValidationWorkers = s.cfg.ValidationWorkers
	}
	if cfg.TransformWorkers <= 0 {
		cfg.TransformWorkers = s.cfg.TransformWorkers
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = s.cfg.BufferSize
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = s.cfg.DefaultTimeoutSeconds
	}
}

// SubmitJob verifies capacity, persists the job, and launches it in the background.
func (s *JobService) SubmitJob(ctx context.Context, cfg models.Config) (*models.Job, error) {
	s.applyServiceDefaults(&cfg)

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, pkgerrors.ErrShuttingDown
	}
	if len(s.cancels) >= s.maxActiveJobs {
		s.mu.Unlock()
		return nil, pkgerrors.ErrCapacityReached
	}

	now := time.Now().UTC()
	job := &models.Job{
		ID:        newJobID(now),
		Status:    models.StatusPending,
		Config:    cfg,
		CreatedAt: now,
		UpdatedAt: now,
		Metrics:   models.Metrics{StageLatency: make(map[string]time.Duration)},
	}
	if err := s.storage.SaveJob(ctx, job); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("failed persisting job: %w", err)
	}

	jobCtx, cancel := context.WithCancelCause(context.Background())
	s.cancels[job.ID] = cancel
	s.inFlight.Add(1)
	s.mu.Unlock()

	snapshot := *job
	go func() {
		defer s.inFlight.Done()
		defer func() {
			s.mu.Lock()
			delete(s.cancels, job.ID)
			s.mu.Unlock()
			cancel(nil)
		}()
		s.runner.Execute(jobCtx, job, nil)
	}()

	return &snapshot, nil
}

// GetJob retrieves a job by ID.
func (s *JobService) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return s.storage.GetJob(ctx, id)
}

// ListJobs returns a page of jobs (newest first) and the total match count.
func (s *JobService) ListJobs(ctx context.Context, opts storage.ListOptions) ([]models.Job, int, error) {
	return s.storage.ListJobs(ctx, opts)
}

// CancelJob signals cancellation to an active job.
func (s *JobService) CancelJob(id string) error {
	s.mu.Lock()
	cancel, running := s.cancels[id]
	s.mu.Unlock()
	if !running {
		return pkgerrors.ErrJobNotRunning
	}
	cancel(ErrCancelledByUser)
	return nil
}

// IsRunning reports whether a job is currently executing in this process.
func (s *JobService) IsRunning(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.cancels[id]
	return ok
}

// DeleteJob removes a finished job's persisted state.
func (s *JobService) DeleteJob(ctx context.Context, id string) error {
	if s.IsRunning(id) {
		return pkgerrors.ErrJobStillRunning
	}
	_, found, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return pkgerrors.ErrJobNotFound
	}
	return s.storage.DeleteJob(ctx, id)
}

// Shutdown stops accepting jobs, cancels every running job, and waits for them to persist their
// terminal (CANCELLED) state, or until ctx expires.
func (s *JobService) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	running := len(s.cancels)
	for _, cancel := range s.cancels {
		cancel(ErrServerShutdown)
	}
	s.mu.Unlock()

	if running > 0 {
		slog.Info("cancelling running jobs for shutdown", "count", running)
	}
	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for %d job(s) to stop: %w", running, ctx.Err())
	}
}

// RecoverInterrupted marks jobs left PENDING or RUNNING by a previous process (crash, kill -9,
// or a shutdown that timed out) as FAILED, since nothing will ever resume them.
func (s *JobService) RecoverInterrupted(ctx context.Context) (int, error) {
	recovered := 0
	for _, status := range []models.JobStatus{models.StatusPending, models.StatusRunning} {
		jobs, _, err := s.storage.ListJobs(ctx, storage.ListOptions{Status: status})
		if err != nil {
			return recovered, err
		}
		for i := range jobs {
			job := &jobs[i]
			if s.IsRunning(job.ID) {
				continue
			}
			now := time.Now().UTC()
			job.Status = models.StatusFailed
			job.Error = "interrupted: the server stopped before the job finished"
			job.UpdatedAt = now
			job.Metrics.EndedAt = &now
			if err := s.storage.SaveJob(ctx, job); err != nil {
				return recovered, err
			}
			recovered++
		}
	}
	return recovered, nil
}
