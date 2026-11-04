package repositories

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// JobRepository defines the contract for persisting and retrieving pipeline job states.
type JobRepository interface {
	Save(ctx context.Context, job *models.Job) error
	FindByID(ctx context.Context, id string) (*models.Job, error)
	FindAll(ctx context.Context) ([]*models.Job, error)
	Delete(ctx context.Context, id string) error
	Close() error
}
