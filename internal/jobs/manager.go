package jobs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

var jobSequence atomic.Uint64

// PipelineExecutor is the behavior required by Job Manager to run a pipeline.
type PipelineExecutor interface {
	Execute(ctx context.Context, job *models.Job, onUpdate func(*models.Job))
}

// Manager coordinates job submission, active concurrency, cancellation, and retrieval.
type Manager struct {
	config        *config.Config
	storage       storage.Repository
	pipeline      PipelineExecutor
	maxActiveJobs int
	mu            sync.Mutex
	cancels       map[string]context.CancelFunc
}

// NewManager constructs a new job Manager instance.
func NewManager(cfg *config.Config, store storage.Repository, pipe PipelineExecutor) *Manager {
	return &Manager{
		config:        cfg,
		storage:       store,
		pipeline:      pipe,
		maxActiveJobs: cfg.MaxActiveJobs,
		cancels:       make(map[string]context.CancelFunc),
	}
}

// SubmitJob checks active quotas, initializes a job, and triggers asynchronous execution.
func (m *Manager) SubmitJob(ctx context.Context, cfg models.Config) (*models.Job, error) {
	m.mu.Lock()
	if len(m.cancels) >= m.maxActiveJobs {
		m.mu.Unlock()
		return nil, ErrCapacityReached
	}

	now := time.Now().UTC()
	seq := jobSequence.Add(1) % 1000
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

	if err := m.storage.SaveJob(ctx, job); err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("persist pending job: %w", err)
	}

	jobCtx, cancel := context.WithCancel(context.Background())
	m.cancels[jobID] = cancel
	m.mu.Unlock()

	metrics.GlobalCollector.IncJobsTotal()
	snapshot := cloneJob(job)

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.cancels, jobID)
			m.mu.Unlock()
		}()
		m.pipeline.Execute(jobCtx, job, func(updated *models.Job) {
			_ = m.storage.SaveJob(context.Background(), updated)
		})
	}()

	return snapshot, nil
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
