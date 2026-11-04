package pipeline

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/aggregation"
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

// Execute runs the full pipeline for a job, publishing updates to storage.
func (p *Pipeline) Execute(parent context.Context, job *models.Job, onUpdate func(*models.Job)) {
	cfg := job.Config
	applyDefaults(&cfg)
	job.Config = cfg

	ctx, cancel := CreateJobContext(parent, cfg.TimeoutSeconds)
	defer cancel()

	slog.Info("pipeline job started", "job_id", job.ID, "sources_count", len(cfg.Sources))

	job.Status = models.StatusRunning
	job.Metrics.StartedAt = time.Now()
	job.Metrics.StageLatency = make(map[string]time.Duration)
	_ = p.storage.SaveJob(ctx, job)
	if onUpdate != nil {
		onUpdate(job)
	}

	// 1. Channel graph setup
	channels := NewPipelineChannels(cfg.BufferSize)

	// 2. Start side-channel listener: Progress Tracking
	progressWG := startProgressTracker(channels.Progress, job, p.collector)

	// 3. Start side-channel listener: Centralized Error Collection
	errWG := startErrorCollector(ctx, channels.Errors, job.ID, p.storage, channels.Progress)

	// 4. Ingestion Stage
	ingestFailures := p.startIngestion(ctx, cfg, channels.Raw, channels.Progress, channels.Errors)

	// 5. Validation Stage
	recLatency := func(stage string, d time.Duration) {
		recordStageLatency(channels.Progress, stage, d)
	}
	validatorWG := startValidationWorkers(ctx, cfg.ValidationWorkers, p.validator, cfg.Rules, channels.Raw, channels.Valid, channels.Errors, recLatency)
	go func() {
		validatorWG.Wait()
		close(channels.Valid)
	}()

	// 6. Transformation Stage
	transformWG := startTransformationWorkers(ctx, cfg.TransformWorkers, p.transformer, cfg.Rules, channels.Valid, channels.Transformed, channels.Errors, recLatency)
	go func() {
		transformWG.Wait()
		close(channels.Transformed)
	}()

	// 7. Aggregation Stage
	agg := aggregation.NewAggregator(cfg.Aggregations)
	p.runAggregationAndExport(ctx, job, agg, channels.Transformed, channels.Result, channels.Progress)

	// Perform file exports
	exportStart := time.Now()
	exportedRecords, _ := p.storage.GetRecords(ctx, job.ID)
	primaryPath, expErr := p.exporter.Export(exportedRecords, cfg.Exports, job.ID)
	recordStageLatency(channels.Progress, "export", time.Since(exportStart))
	if expErr != nil {
		slog.Error("pipeline export failed", "job_id", job.ID, "error", expErr)
		channels.Errors <- models.RecordError{Stage: "export", Message: expErr.Error(), At: time.Now()}
	} else {
		job.ResultsPath = primaryPath
		channels.Progress <- progressMsg{kind: progressExported, count: int64(len(exportedRecords))}
	}

	// Close error channel after pipeline workers finish
	close(channels.Errors)
	errWG.Wait()

	dlq := cfg.DLQPath
	if dlq == "" {
		dlq = cfg.DeadLetterPath
	}
	if dlq != "" {
		exportDeadLetterRecords(ctx, dlq, job.ID, p.storage)
	}

	close(channels.Progress)
	progressWG.Wait()

	// 8. Finalize job status and metrics
	p.finalizeJob(ctx, parent, job, agg, expErr, int(ingestFailures.Load()))
	slog.Info("pipeline job finished",
		"job_id", job.ID,
		"status", job.Status,
		"processed", job.Metrics.Processed,
		"valid", job.Metrics.Valid,
		"rejected", job.Metrics.Rejected,
		"errors", job.Metrics.Errors,
	)
	if onUpdate != nil {
		onUpdate(job)
	}
}

// Engine wraps Pipeline for backward compatibility.
type Engine struct {
	Store  *storage.Store
	Client *http.Client
}

// Run executes the pipeline using the underlying Pipeline coordinator.
func (e *Engine) Run(parent context.Context, j *models.Job, update func(*models.Job)) {
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
func validate(r models.Record, rules []models.Rule) error {
	v := validation.NewValidator()
	return v.Validate(r, rules)
}

func transform(r models.Record, rules []models.Rule) (models.Record, error) {
	t := transformation.NewTransformer()
	return t.Transform(r, rules)
}
