package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/config"
	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/repository"
)

var (
	ErrJobNotFound     = errors.New("pipeline job not found")
	ErrJobNotRunning   = errors.New("pipeline job is not running")
	ErrJobStillRunning = errors.New("pipeline job is still running; cancel it first")
	ErrCapacityReached = errors.New("system capacity reached: maximum concurrent jobs active")
)

// Manager coordinates job submission, execution concurrency, cancellation, and retrieval.
type Manager struct {
	repo          repository.Repository
	executor      *Executor
	maxActiveJobs int
	mu            sync.Mutex
	cancels       map[string]context.CancelFunc
}

// NewManager creates a new job Manager instance.
func NewManager(cfg *config.Config, repo repository.Repository, executor *Executor) *Manager {
	return &Manager{
		repo:          repo,
		executor:      executor,
		maxActiveJobs: cfg.MaxActiveJobs,
		cancels:       make(map[string]context.CancelFunc),
	}
}

// SubmitJob validates concurrency limits, registers a new job, and triggers asynchronous execution.
func (m *Manager) SubmitJob(ctx context.Context, cfg models.Config) (*models.Job, error) {
	m.mu.Lock()
	if len(m.cancels) >= m.maxActiveJobs {
		m.mu.Unlock()
		return nil, ErrCapacityReached
	}

	now := time.Now().UTC()
	jobID := now.Format("20060102T150405.000000000")
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

	if err := m.repo.SaveJob(ctx, job); err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("persist pending job: %w", err)
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	m.cancels[jobID] = cancel
	m.mu.Unlock()

	metrics.GlobalCollector.IncJobsTotal()

	jobSnapshot := cloneJob(job)

	// Launch background pipeline execution
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.cancels, jobID)
			m.mu.Unlock()
		}()
		m.executor.Execute(jobCtx, job, func(updated *models.Job) {
			_ = m.repo.SaveJob(context.Background(), updated)
		})
	}()

	return jobSnapshot, nil
}

func cloneJob(j *models.Job) *models.Job {
	if j == nil {
		return nil
	}
	var c models.Job
	c = *j
	if j.Aggregates != nil {
		c.Aggregates = make(map[string]any, len(j.Aggregates))
		for k, v := range j.Aggregates {
			c.Aggregates[k] = v
		}
	}
	if j.Metrics.StageLatency != nil {
		c.Metrics.StageLatency = make(map[string]time.Duration, len(j.Metrics.StageLatency))
		for k, v := range j.Metrics.StageLatency {
			c.Metrics.StageLatency[k] = v
		}
	}
	return &c
}

// GetJob retrieves a job by ID.
func (m *Manager) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return m.repo.GetJob(ctx, id)
}

// ListJobs returns all recorded jobs.
func (m *Manager) ListJobs(ctx context.Context) ([]models.Job, error) {
	return m.repo.ListJobs(ctx)
}

// CancelJob signals cooperative cancellation to an active job.
func (m *Manager) CancelJob(id string) error {
	m.mu.Lock()
	cancel, exists := m.cancels[id]
	m.mu.Unlock()

	if !exists {
		return ErrJobNotRunning
	}
	cancel()
	return nil
}

// DeleteJob removes a job, its persisted records/errors, and any exported artifacts.
func (m *Manager) DeleteJob(ctx context.Context, id string) error {
	m.mu.Lock()
	_, running := m.cancels[id]
	m.mu.Unlock()

	if running {
		return ErrJobStillRunning
	}

	job, found, err := m.repo.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return ErrJobNotFound
	}

	// Clean up any export files created by this job
	for _, target := range job.Config.Exports {
		path := target.Path
		if path == "" && target.Type != "" {
			path = filepath.Join(m.executor.ExportDir, job.ID+"."+target.Type)
		}
		if path != "" {
			_ = os.Remove(path)
		}
	}

	return m.repo.DeleteJob(ctx, id)
}

// GetProgress computes real-time progress metrics for a job.
func (m *Manager) GetProgress(ctx context.Context, id string) (*models.Progress, bool, error) {
	job, found, err := m.repo.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	metrics := job.Metrics
	percent := 0.0
	if metrics.Discovered > 0 {
		percent = float64(metrics.Processed+metrics.Rejected) / float64(metrics.Discovered) * 100.0
	}
	if job.Status == models.StatusCompleted || job.Status == models.StatusFailed || job.Status == models.StatusCancelled {
		percent = 100.0
	}

	pending := metrics.Discovered - metrics.Processed - metrics.Rejected
	if pending < 0 {
		pending = 0
	}

	return &models.Progress{
		JobID:            id,
		Status:           job.Status,
		Percent:          percent,
		Processed:        metrics.Processed,
		Discovered:       metrics.Discovered,
		Pending:          pending,
		Valid:            metrics.Valid,
		Rejected:         metrics.Rejected,
		ErrorCount:       metrics.Errors,
		StartedAt:        metrics.StartedAt,
		EndedAt:          metrics.EndedAt,
		RecordsPerSecond: metrics.RecordsPerSecond,
		StageLatency:     metrics.StageLatency,
	}, true, nil
}

// GetResults retrieves processed records, computed aggregates, and export path for a job.
func (m *Manager) GetResults(ctx context.Context, id string) (map[string]any, bool, error) {
	job, found, err := m.repo.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	records, err := m.repo.GetRecords(ctx, id)
	if err != nil {
		return nil, false, err
	}

	return map[string]any{
		"job_id":       id,
		"status":       job.Status,
		"records":      records,
		"aggregations": job.Aggregates,
		"export_path":  job.ResultsPath,
	}, true, nil
}

// GetErrors retrieves quarantined errors recorded during job processing.
func (m *Manager) GetErrors(ctx context.Context, id string) ([]models.RecordError, bool, error) {
	_, found, err := m.repo.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	errs, err := m.repo.GetErrors(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return errs, true, nil
}
