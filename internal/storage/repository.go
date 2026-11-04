package storage

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// ListOptions filters and paginates job listings. A Limit <= 0 returns every match.
type ListOptions struct {
	Status models.JobStatus
	Offset int
	Limit  int
}

// Repository specifies the durable persistence contract for pipeline jobs, records, and errors.
//
// Listing methods return the requested page together with the total number of matching rows.
// Jobs are listed newest first; records and errors are returned in insertion order.
type Repository interface {
	SaveJob(ctx context.Context, job *models.Job) error
	GetJob(ctx context.Context, id string) (*models.Job, bool, error)
	ListJobs(ctx context.Context, opts ListOptions) ([]models.Job, int, error)
	DeleteJob(ctx context.Context, id string) error

	AddErrors(ctx context.Context, jobID string, errs []models.RecordError) error
	GetErrors(ctx context.Context, jobID string, offset, limit int) ([]models.RecordError, int, error)

	SaveRecords(ctx context.Context, jobID string, records []models.Record) error
	GetRecords(ctx context.Context, jobID string, offset, limit int) ([]models.Record, int, error)

	Close() error
}

// page slices a list using offset/limit semantics shared by all repositories.
func page[T any](list []T, offset, limit int) []T {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(list) {
		return []T{}
	}
	end := len(list)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	out := make([]T, end-offset)
	copy(out, list[offset:end])
	return out
}
