package services

import (
	"context"
	"log/slog"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
)

// PurgeExpired deletes finished jobs (COMPLETED, FAILED, CANCELLED) last updated before cutoff,
// together with their records, errors, and exported files. Running jobs are never touched.
func (s *PipelineService) PurgeExpired(ctx context.Context, cutoff time.Time) (int, error) {
	purged := 0
	for _, status := range []models.JobStatus{models.StatusCompleted, models.StatusFailed, models.StatusCancelled} {
		jobs, _, err := s.Jobs.ListJobs(ctx, storage.ListOptions{Status: status})
		if err != nil {
			return purged, err
		}
		for i := range jobs {
			job := &jobs[i]
			if !job.UpdatedAt.Before(cutoff) || s.Jobs.IsRunning(job.ID) {
				continue
			}
			if err := s.DeleteJob(ctx, job.ID); err != nil {
				slog.Warn("retention: failed deleting job", "job_id", job.ID, "error", err)
				continue
			}
			purged++
		}
	}
	return purged, nil
}

// RunRetention purges expired jobs every interval until ctx is cancelled. retention <= 0 disables it.
func (s *PipelineService) RunRetention(ctx context.Context, retention, interval time.Duration) {
	if retention <= 0 {
		return
	}
	if interval <= 0 {
		interval = time.Hour
	}
	purge := func() {
		n, err := s.PurgeExpired(ctx, time.Now().Add(-retention))
		if err != nil && ctx.Err() == nil {
			slog.Error("retention sweep failed", "error", err)
		} else if n > 0 {
			slog.Info("retention sweep purged expired jobs", "count", n, "retention", retention.String())
		}
	}
	purge()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			purge()
		}
	}
}
