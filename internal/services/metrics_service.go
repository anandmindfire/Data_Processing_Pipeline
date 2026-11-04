package services

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// MetricsService provides job metrics calculation, progress polling, and error retrieval.
type MetricsService struct {
	storage storage.Repository
}

// NewMetricsService constructs a new MetricsService.
func NewMetricsService(store storage.Repository) *MetricsService {
	return &MetricsService{storage: store}
}

// GetProgress calculates and returns progress details for a job.
func (s *MetricsService) GetProgress(ctx context.Context, id string) (*models.Progress, bool, error) {
	job, found, err := s.storage.GetJob(ctx, id)
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
		Error:            job.Error,
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

// GetErrors retrieves a page of error logs for a job, the total error count, and whether the job exists.
func (s *MetricsService) GetErrors(ctx context.Context, id string, offset, limit int) ([]models.RecordError, int, bool, error) {
	_, found, err := s.storage.GetJob(ctx, id)
	if err != nil || !found {
		return nil, 0, false, err
	}
	errs, total, err := s.storage.GetErrors(ctx, id, offset, limit)
	return errs, total, true, err
}

func calculatePercent(m models.Metrics, status models.JobStatus) float64 {
	if status == models.StatusCompleted {
		return 100.0
	}
	if m.Discovered <= 0 {
		return 0.0
	}
	totalAccounted := m.Processed + m.Rejected
	p := (float64(totalAccounted) / float64(m.Discovered)) * 100.0
	if p > 100.0 {
		return 100.0
	}
	return p
}
