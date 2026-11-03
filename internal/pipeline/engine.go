package pipeline

import (
	"context"
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline/stages"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
)

// Engine is retained for backward compatibility with existing tests.
type Engine struct {
	Store  *storage.Store
	Client *http.Client
}

// Run executes the pipeline using the underlying Executor.
func (e *Engine) Run(parent context.Context, j *domain.Job, update func(*domain.Job)) {
	repo := e.Store.Repository()
	exec := NewExecutor(repo, e.Client, "exports", nil)
	exec.Execute(parent, j, update)
}

// Validation and transformation helpers exposed for legacy unit tests.
func validate(r domain.Record, rules []domain.Rule) error {
	return stages.ValidateRecord(r, rules)
}

func transform(r domain.Record, rules []domain.Rule) (models.Record, error) {
	return stages.TransformRecord(r, rules)
}
