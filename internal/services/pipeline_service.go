package services

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// PipelineService coordinates all pipeline subsystem services.
type PipelineService struct {
	Jobs    *JobService
	Metrics *MetricsService
	Exports *ExportService
}

// NewPipelineService constructs a unified PipelineService.
func NewPipelineService(cfg *config.Config, store storage.Repository, runner PipelineRunner) *PipelineService {
	return &PipelineService{
		Jobs:    NewJobService(cfg, store, runner),
		Metrics: NewMetricsService(store),
		Exports: NewExportService(cfg, store),
	}
}

// Results is a page of a job's processed records together with its aggregations and export links.
type Results struct {
	JobID        string           `json:"job_id"`
	Status       models.JobStatus `json:"status"`
	Aggregations map[string]any   `json:"aggregations"`
	ExportPath   string           `json:"export_path,omitempty"`
	Artifacts    []string         `json:"artifacts,omitempty"`
	TotalRecords int              `json:"total_records"`
	Offset       int              `json:"offset"`
	Limit        int              `json:"limit"`
	Records      []models.Record  `json:"records"`
}

// SubmitJob delegates job submission to JobService.
func (s *PipelineService) SubmitJob(ctx context.Context, cfg models.JobSpec) (*models.Job, error) {
	return s.Jobs.SubmitJob(ctx, cfg)
}

// GetJob delegates job retrieval to JobService.
func (s *PipelineService) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	return s.Jobs.GetJob(ctx, id)
}

// ListJobs delegates job listing to JobService.
func (s *PipelineService) ListJobs(ctx context.Context, opts storage.ListOptions) ([]models.Job, int, error) {
	return s.Jobs.ListJobs(ctx, opts)
}

// CancelJob delegates job cancellation to JobService.
func (s *PipelineService) CancelJob(ctx context.Context, id string) error {
	return s.Jobs.CancelJob(ctx, id)
}

// Shutdown cancels running jobs and waits for them to finish.
func (s *PipelineService) Shutdown(ctx context.Context) error {
	return s.Jobs.Shutdown(ctx)
}

// DeleteJob removes a finished job, its data, errors, and export files.
func (s *PipelineService) DeleteJob(ctx context.Context, id string) error {
	job, found, err := s.Jobs.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return ErrJobNotFound
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

// GetResults returns a page of processed records plus aggregations and export paths.
func (s *PipelineService) GetResults(ctx context.Context, id string, offset, limit int) (*Results, bool, error) {
	job, found, err := s.Jobs.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}
	records, total, err := s.Exports.GetRecords(ctx, id, offset, limit)
	if err != nil {
		return nil, false, err
	}
	return &Results{
		JobID:        id,
		Status:       job.Status,
		Aggregations: job.Aggregates,
		ExportPath:   job.ResultsPath,
		Artifacts:    job.Artifacts,
		TotalRecords: total,
		Offset:       offset,
		Limit:        limit,
		Records:      records,
	}, true, nil
}

// GetErrors returns a page of errors recorded for a job and the total error count.
func (s *PipelineService) GetErrors(ctx context.Context, id string, offset, limit int) ([]models.RecordError, int, bool, error) {
	return s.Metrics.GetErrors(ctx, id, offset, limit)
}
