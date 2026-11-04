package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	pkgerrors "github.com/anandmindfire/data-processing-pipeline/pkg/errors"
)

var jobSeq atomic.Uint64

// PipelineRunner defines the interface for executing a pipeline job.
type PipelineRunner interface {
	Execute(ctx context.Context, job *models.Job, onUpdate func(*models.Job))
}

// JobService manages job execution lifecycle, concurrency constraints, and states.
type JobService struct {
	cfg           *configs.Config
	storage       storage.Repository
	runner        PipelineRunner
	maxActiveJobs int
	mu            sync.Mutex
	cancels       map[string]context.CancelFunc
}

// NewJobService creates a new JobService instance.
func NewJobService(cfg *configs.Config, store storage.Repository, runner PipelineRunner) *JobService {
	return &JobService{
		cfg:           cfg,
		storage:       store,
		runner:        runner,
		maxActiveJobs: cfg.MaxActiveJobs,
		cancels:       make(map[string]context.CancelFunc),
	}
}

// SubmitJob verifies active job limits and launches background pipeline processing.
func (s *JobService) SubmitJob(ctx context.Context, cfg models.Config) (*models.Job, error) {
	s.mu.Lock()
	if len(s.cancels) >= s.maxActiveJobs {
		s.mu.Unlock()
		return nil, pkgerrors.ErrCapacityReached
	}

	now := time.Now().UTC()
	seq := jobSeq.Add(1) % 1000
	jobID := fmt.Sprintf("%s-%03d", now.Format("20060102T150405.000000000"), seq)
	job := &models.Job{
		ID:        jobID,
		Status:    models.StatusPending,
		Config:    cfg,
		CreatedAt: now,
		UpdatedAt: now,
		Metrics: models.Metrics{
			StageLatency: make(map[string]time.Duration),
		},
	}

	if err := s.storage.SaveJob(ctx, job); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("failed persisting job: %w", err)
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	s.cancels[jobID] = cancel
	s.mu.Unlock()

	jobSnapshot := *job

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.cancels, jobID)
			s.mu.Unlock()
		}()
		s.runner.Execute(jobCtx, job, func(updated *models.Job) {
			_ = s.storage.SaveJob(context.Background(), updated)
		})
	}()

	return &jobSnapshot, nil
}

// GetJob retrieves a job by ID.
func (s *JobService) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return s.storage.GetJob(ctx, id)
}

// ListJobs returns all recorded jobs.
func (s *JobService) ListJobs(ctx context.Context) ([]models.Job, error) {
	return s.storage.ListJobs(ctx)
}

// CancelJob signals cancellation to an active job.
func (s *JobService) CancelJob(id string) error {
	s.mu.Lock()
	cancel, running := s.cancels[id]
	s.mu.Unlock()

	if !running {
		return pkgerrors.ErrJobNotRunning
	}
	cancel()
	return nil
}

// DeleteJob removes a finished job and cleans up associated resources.
func (s *JobService) DeleteJob(ctx context.Context, id string) error {
	s.mu.Lock()
	_, running := s.cancels[id]
	s.mu.Unlock()

	if running {
		return pkgerrors.ErrJobStillRunning
	}

	job, found, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return pkgerrors.ErrJobNotFound
	}

	_ = job
	return s.storage.DeleteJob(ctx, id)
}
