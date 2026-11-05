package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
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
//
// Several replicas may share one job store. Each job is owned by the replica that accepted it
// (instanceID); the owner heartbeats its jobs and polls for cancellation requests that other
// replicas record in the store, and every replica fails jobs whose owner stopped heartbeating.
// MAX_ACTIVE_JOBS is enforced per replica.
type JobService struct {
	cfg           *config.Config
	storage       storage.Repository
	runner        PipelineRunner
	maxActiveJobs int
	instanceID    string
	lease         time.Duration

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
	lease := cfg.JobLease
	if lease <= 0 {
		lease = 30 * time.Second
	}
	instance := cfg.InstanceID
	if instance == "" {
		instance = DefaultInstanceID()
	}
	return &JobService{
		cfg:           cfg,
		storage:       store,
		runner:        runner,
		maxActiveJobs: maxActive,
		instanceID:    instance,
		lease:         lease,
		cancels:       make(map[string]context.CancelCauseFunc),
	}
}

// DefaultInstanceID returns "<hostname>-<random>", unique per process.
func DefaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "instance"
	}
	var b [3]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}

// InstanceID identifies this replica as the owner of the jobs it runs.
func (s *JobService) InstanceID() string { return s.instanceID }

// newJobID returns a sortable, collision-resistant ID: UTC timestamp + 48 random bits.
func newJobID(now time.Time) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return now.Format("20060102T150405.000Z") + "-" + hex.EncodeToString(b[:])
}

// applyServiceDefaults fills unset tuning knobs from the service configuration so the stored job
// shows the effective values it ran with.
func (s *JobService) applyServiceDefaults(cfg *models.JobSpec) {
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
func (s *JobService) SubmitJob(ctx context.Context, cfg models.JobSpec) (*models.Job, error) {
	s.applyServiceDefaults(&cfg)

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, ErrShuttingDown
	}
	if len(s.cancels) >= s.maxActiveJobs {
		s.mu.Unlock()
		return nil, ErrCapacityReached
	}

	now := time.Now().UTC()
	job := &models.Job{
		ID:        newJobID(now),
		Status:    models.StatusPending,
		Spec:      cfg,
		CreatedAt: now,
		UpdatedAt: now,
		Owner:     s.instanceID,
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

// CancelJob cancels an active job. A job running on this replica is cancelled immediately; a job
// running on another replica is flagged in the store and its owner stops it within about a second.
func (s *JobService) CancelJob(ctx context.Context, id string) error {
	s.mu.Lock()
	cancel, running := s.cancels[id]
	s.mu.Unlock()
	if running {
		cancel(ErrCancelledByUser)
		return nil
	}
	found, active, err := s.storage.RequestCancel(ctx, id)
	switch {
	case err != nil:
		return err
	case !found:
		return ErrJobNotFound
	case !active:
		return ErrJobNotRunning
	}
	return nil
}

// Accepting reports whether the service still accepts new jobs (false once shutdown began).
func (s *JobService) Accepting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closing
}

// IsRunning reports whether a job is currently executing in this process.
func (s *JobService) IsRunning(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.cancels[id]
	return ok
}

// DeleteJob removes a finished job's persisted state. Jobs still PENDING or RUNNING (on any
// replica) must be cancelled first.
func (s *JobService) DeleteJob(ctx context.Context, id string) error {
	if s.IsRunning(id) {
		return ErrJobStillRunning
	}
	job, found, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return ErrJobNotFound
	}
	if !job.Status.IsTerminal() {
		return ErrJobStillRunning
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

// RecoverInterrupted fails PENDING/RUNNING jobs whose owner has not heartbeated since cutoff
// (crash, kill -9, lost node, or a shutdown that timed out), since nothing will resume them.
// With a single-instance store the caller passes "now" at startup; with a shared store it passes
// now-lease so jobs of healthy replicas are left alone.
func (s *JobService) RecoverInterrupted(ctx context.Context, cutoff time.Time) (int, error) {
	ids, err := s.storage.FailStaleJobs(ctx, cutoff, "interrupted: the instance running the job stopped before it finished")
	if len(ids) > 0 {
		slog.Warn("failed jobs abandoned by a stopped instance", "count", len(ids), "job_ids", ids)
	}
	return len(ids), err
}

// RunCoordinator keeps this replica's jobs alive in a shared store until ctx is cancelled:
// every second (or lease/10 for short leases) it applies cancellation requests made through other replicas, every lease/6 it
// heartbeats its running jobs, and every lease/2 it fails jobs whose owner went silent.
func (s *JobService) RunCoordinator(ctx context.Context) {
	interval := min(time.Second, max(s.lease/10, 10*time.Millisecond))
	tick := time.NewTicker(interval)
	defer tick.Stop()
	heartbeatEvery := max(s.lease/6, interval)
	reclaimEvery := max(s.lease/2, interval)
	lastHeartbeat, lastReclaim := time.Time{}, time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			ids := s.runningIDs()
			if len(ids) > 0 {
				s.applyRemoteCancels(ctx, ids)
				if now.Sub(lastHeartbeat) >= heartbeatEvery {
					if err := s.storage.Heartbeat(ctx, s.instanceID, ids, now); err != nil && ctx.Err() == nil {
						slog.Warn("job heartbeat failed", "error", err)
					}
					lastHeartbeat = now
				}
			}
			if now.Sub(lastReclaim) >= reclaimEvery {
				if _, err := s.RecoverInterrupted(ctx, now.Add(-s.lease)); err != nil && ctx.Err() == nil {
					slog.Warn("stale job recovery failed", "error", err)
				}
				lastReclaim = now
			}
		}
	}
}

func (s *JobService) runningIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.cancels))
	for id := range s.cancels {
		ids = append(ids, id)
	}
	return ids
}

func (s *JobService) applyRemoteCancels(ctx context.Context, ids []string) {
	flagged, err := s.storage.CancelRequested(ctx, ids)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("polling cancellation requests failed", "error", err)
		}
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range flagged {
		if cancel, ok := s.cancels[id]; ok {
			slog.Info("cancelling job on request from another instance", "job_id", id)
			cancel(ErrCancelledByUser)
		}
	}
}
