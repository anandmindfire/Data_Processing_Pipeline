package pipeline

import (
	"context"
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"github.com/anandkr47/data-processing-pipeline/internal/export"
	"github.com/anandkr47/data-processing-pipeline/internal/ingestion"
	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
	"github.com/anandkr47/data-processing-pipeline/internal/transformation"
	"github.com/anandkr47/data-processing-pipeline/internal/validation"
)

// Engine is retained for backward compatibility with existing integration tests.
type Engine struct {
	Store  *storage.Store
	Client *http.Client
}

// Run executes the pipeline using the underlying Pipeline coordinator.
func (e *Engine) Run(parent context.Context, j *domain.Job, update func(*domain.Job)) {
	repo := e.Store.Repository()
	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}

	pipe := NewPipeline(
		ingestion.NewReader(client),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter("exports"),
		repo,
		metrics.GlobalCollector,
	)
	pipe.Execute(parent, j, update)
}

// Helpers exposed for unit test compatibility.
func validate(r domain.Record, rules []domain.Rule) error {
	v := validation.NewValidator()
	return v.Validate(r, rules)
}

func transform(r domain.Record, rules []domain.Rule) (models.Record, error) {
	t := transformation.NewTransformer()
	return t.Transform(r, rules)
}
