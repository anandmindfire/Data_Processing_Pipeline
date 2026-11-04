package services

import (
	"context"
	"os"
	"path/filepath"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// ExportService manages access to exported dataset artifacts and cleanup.
type ExportService struct {
	cfg     *configs.Config
	storage storage.Repository
}

// NewExportService constructs a new ExportService.
func NewExportService(cfg *configs.Config, store storage.Repository) *ExportService {
	return &ExportService{
		cfg:     cfg,
		storage: store,
	}
}

// GetRecords retrieves stored records for a completed job.
func (s *ExportService) GetRecords(ctx context.Context, id string) ([]models.Record, bool, error) {
	_, found, err := s.storage.GetJob(ctx, id)
	if err != nil || !found {
		return nil, false, err
	}
	records, err := s.storage.GetRecords(ctx, id)
	return records, true, err
}

// CleanupJobExports removes exported output files and DLQ artifacts for a job.
func (s *ExportService) CleanupJobExports(job *models.Job) {
	for _, target := range job.Config.Exports {
		path := target.Path
		if path == "" && target.Type != "" {
			path = filepath.Join(s.cfg.ExportDir, job.ID+"."+target.Type)
		}
		if path != "" {
			_ = os.Remove(path)
		}
	}
	if job.Config.DLQPath != "" {
		_ = os.Remove(job.Config.DLQPath)
	}
}
