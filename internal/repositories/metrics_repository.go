package repositories

import (
	"context"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// MetricsRepository defines the contract for persisting and retrieving telemetry metrics.
type MetricsRepository interface {
	Save(ctx context.Context, jobID string, m models.Metrics) error
	FindByJobID(ctx context.Context, jobID string) (*models.Metrics, error)
	DeleteByJobID(ctx context.Context, jobID string) error
}
