package pipeline

import (
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// Pipeline coordinates the execution of an end-to-end data processing workflow.
type Pipeline struct {
	reader      ingestion.Reader
	validator   validation.Validator
	transformer transformation.Transformer
	exporter    export.Exporter
	storage     storage.Repository
	collector   *metrics.Collector
}

// NewPipeline constructs a Pipeline via dependency injection.
func NewPipeline(
	reader ingestion.Reader,
	validator validation.Validator,
	transformer transformation.Transformer,
	exporter export.Exporter,
	store storage.Repository,
	collector *metrics.Collector,
) *Pipeline {
	if collector == nil {
		collector = metrics.GlobalCollector
	}
	return &Pipeline{
		reader:      reader,
		validator:   validator,
		transformer: transformer,
		exporter:    exporter,
		storage:     store,
		collector:   collector,
	}
}

// applyDefaults populates missing worker and timeout settings with sensible defaults.
func applyDefaults(cfg *models.Config) {
	if cfg.ValidationWorkers <= 0 {
		cfg.ValidationWorkers = 4
	}
	if cfg.TransformWorkers <= 0 {
		cfg.TransformWorkers = 4
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 128
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 300
	}
}
