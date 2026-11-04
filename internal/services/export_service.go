package services

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// ExportService manages access to exported dataset artifacts and cleanup.
type ExportService struct {
	cfg     *config.Config
	storage storage.Repository
}

// NewExportService constructs a new ExportService.
func NewExportService(cfg *config.Config, store storage.Repository) *ExportService {
	return &ExportService{cfg: cfg, storage: store}
}

// GetRecords retrieves a page of stored records for a job and the total record count.
func (s *ExportService) GetRecords(ctx context.Context, id string, offset, limit int) ([]models.Record, int, error) {
	return s.storage.GetRecords(ctx, id, offset, limit)
}

// CleanupJobExports removes the files a job published. Only paths inside the export directory
// are ever deleted, so a tampered job record cannot be used to remove arbitrary files.
func (s *ExportService) CleanupJobExports(job *models.Job) {
	root, err := filepath.Abs(s.cfg.ExportDir)
	if err != nil {
		return
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	for _, path := range job.Artifacts {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		rel, err := filepath.Rel(root, abs)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			slog.Warn("refusing to delete artifact outside export directory", "job_id", job.ID, "path", path)
			continue
		}
		if err := os.Remove(abs); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("failed removing job artifact", "job_id", job.ID, "path", abs, "error", err)
		}
	}
}
