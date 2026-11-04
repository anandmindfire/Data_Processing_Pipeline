package repositories

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// ErrorRepository defines the contract for persisting and retrieving record execution errors.
type ErrorRepository interface {
	Save(ctx context.Context, jobID string, recErr models.RecordError) error
	FindByJobID(ctx context.Context, jobID string) ([]models.RecordError, error)
	DeleteByJobID(ctx context.Context, jobID string) error
}
