package storage

import (
	"context"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Repository specifies the durable persistence contract for pipeline jobs, records, and errors.
type Repository interface {
	SaveJob(ctx context.Context, job *models.Job) error
	GetJob(ctx context.Context, id string) (*models.Job, bool, error)
	ListJobs(ctx context.Context) ([]models.Job, error)
	DeleteJob(ctx context.Context, id string) error

	AddError(ctx context.Context, jobID string, err models.RecordError) error
	GetErrors(ctx context.Context, jobID string) ([]models.RecordError, error)

	SaveRecord(ctx context.Context, jobID string, record models.Record) error
	GetRecords(ctx context.Context, jobID string) ([]models.Record, error)

	Close() error
}

