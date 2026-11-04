package services

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	pkgerrors "github.com/anandmindfire/data-processing-pipeline/pkg/errors"
)

// PipelineService coordinates all pipeline subsystem services.
type PipelineService struct {
	Jobs    *JobService
	Metrics *MetricsService
	Exports *ExportService
}

// NewPipelineService constructs a unified PipelineService.
func NewPipelineService(cfg *configs.Config, store storage.Repository, runner PipelineRunner) *PipelineService {
	return &PipelineService{
		Jobs:    NewJobService(cfg, store, runner),
		Metrics: NewMetricsService(store),
		Exports: NewExportService(cfg, store),
	}
}

// SubmitJob delegates job submission to JobService.
func (s *PipelineService) SubmitJob(ctx context.Context, cfg models.Config) (*models.Job, error) {
	return s.Jobs.SubmitJob(ctx, cfg)
}

// GetJob delegates job retrieval to JobService.
func (s *PipelineService) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return s.Jobs.GetJob(ctx, id)
}

// ListJobs delegates job listing to JobService.
func (s *PipelineService) ListJobs(ctx context.Context) ([]models.Job, error) {
	return s.Jobs.ListJobs(ctx)
}

// CancelJob delegates job cancellation to JobService.
func (s *PipelineService) CancelJob(id string) error {
	return s.Jobs.CancelJob(id)
}

// DeleteJob removes a finished job, its data, errors, and export files.
func (s *PipelineService) DeleteJob(ctx context.Context, id string) error {
	job, found, err := s.Jobs.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return pkgerrors.ErrJobNotFound
	}
	if err := s.Jobs.DeleteJob(ctx, id); err != nil {
		return err
	}
	s.Exports.CleanupJobExports(job)
	return nil
}

// GetProgress delegates progress tracking calculation to MetricsService.
func (s *PipelineService) GetProgress(ctx context.Context, id string) (*models.Progress, bool, error) {
	return s.Metrics.GetProgress(ctx, id)
}

// GetResults retrieves execution output records, aggregates, and paths for a job.
func (s *PipelineService) GetResults(ctx context.Context, id string) (map[string]any, bool, error) {
	job, found, err := s.Jobs.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}
	records, _, err := s.Exports.GetRecords(ctx, id)
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

// GetErrors delegates error logs retrieval to MetricsService.
func (s *PipelineService) GetErrors(ctx context.Context, id string) ([]models.RecordError, bool, error) {
	return s.Metrics.GetErrors(ctx, id)
}
