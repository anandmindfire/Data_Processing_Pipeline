package jobs

import (
	"context"
	"os"
	"path/filepath"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// GetJob loads job details by ID.
func (m *Manager) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return m.storage.GetJob(ctx, id)
}

// ListJobs retrieves all registered jobs.
func (m *Manager) ListJobs(ctx context.Context) ([]models.Job, error) {
	return m.storage.ListJobs(ctx)
}

// CancelJob triggers cooperative context cancellation for an active job.
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

// DeleteJob removes a finished job, its data, errors, and export files.
func (m *Manager) DeleteJob(ctx context.Context, id string) error {
	m.mu.Lock()
	_, running := m.cancels[id]
	m.mu.Unlock()

	if running {
		return ErrJobStillRunning
	}

	job, found, err := m.storage.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return ErrJobNotFound
	}

	m.cleanupExports(job)
	return m.storage.DeleteJob(ctx, id)
}

func (m *Manager) cleanupExports(job *models.Job) {
	for _, target := range job.Config.Exports {
		path := target.Path
		if path == "" && target.Type != "" {
			path = filepath.Join(m.config.ExportDir, job.ID+"."+target.Type)
		}
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

// GetProgress calculates current percentage and processing metrics.
func (m *Manager) GetProgress(ctx context.Context, id string) (*models.Progress, bool, error) {
	job, found, err := m.storage.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	met := job.Metrics
	percent := calculatePercent(met, job.Status)
	pending := met.Discovered - met.Processed - met.Rejected
	if pending < 0 {
		pending = 0
	}

	return &models.Progress{
		JobID:            id,
		Status:           job.Status,
		Percent:          percent,
		Processed:        met.Processed,
		Discovered:       met.Discovered,
		Pending:          pending,
		Valid:            met.Valid,
		Rejected:         met.Rejected,
		ErrorCount:       met.Errors,
		StartedAt:        met.StartedAt,
		EndedAt:          met.EndedAt,
		RecordsPerSecond: met.RecordsPerSecond,
		StageLatency:     met.StageLatency,
	}, true, nil
}

func calculatePercent(met models.Metrics, status models.JobStatus) float64 {
	if status == models.StatusCompleted || status == models.StatusFailed || status == models.StatusCancelled {
		return 100.0
	}
	if met.Discovered > 0 {
		return float64(met.Processed+met.Rejected) / float64(met.Discovered) * 100.0
	}
	return 0.0
}

// GetResults fetches processed records, aggregations, and export paths.
func (m *Manager) GetResults(ctx context.Context, id string) (map[string]any, bool, error) {
	job, found, err := m.storage.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	records, err := m.storage.GetRecords(ctx, id)
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

// GetErrors returns quarantined errors for a given job.
func (m *Manager) GetErrors(ctx context.Context, id string) ([]models.RecordError, bool, error) {
	_, found, err := m.storage.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}

	errs, err := m.storage.GetErrors(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return errs, true, nil
}

