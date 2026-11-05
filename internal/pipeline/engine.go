package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// Fallback values used when neither the job spec nor the service configuration provides one.
const (
	DefaultWorkers          = 4
	DefaultBufferSize       = 128
	DefaultTimeoutSeconds   = 300
	DefaultBatchSize        = 500
	DefaultProgressInterval = 500 * time.Millisecond
	DefaultFlushInterval    = 250 * time.Millisecond
)

// Pipeline coordinates the execution of an end-to-end data processing workflow.
type Pipeline struct {
	reader           ingestion.Reader
	validator        validation.Validator
	transformer      transformation.Transformer
	exporter         export.Exporter
	storage          storage.Repository
	collector        *metrics.Collector
	progressInterval time.Duration
	flushInterval    time.Duration
}

// Option customizes a Pipeline.
type Option func(*Pipeline)

// WithProgressInterval sets how often live progress snapshots are persisted (<= 0 disables them).
func WithProgressInterval(d time.Duration) Option {
	return func(p *Pipeline) { p.progressInterval = d }
}

// WithFlushInterval sets the maximum time a partial record/error batch waits before being persisted.
func WithFlushInterval(d time.Duration) Option {
	return func(p *Pipeline) {
		if d > 0 {
			p.flushInterval = d
		}
	}
}

// NewPipeline constructs a Pipeline via dependency injection.
func NewPipeline(
	reader ingestion.Reader,
	validator validation.Validator,
	transformer transformation.Transformer,
	exporter export.Exporter,
	store storage.Repository,
	collector *metrics.Collector,
	opts ...Option,
) *Pipeline {
	if collector == nil {
		collector = metrics.GlobalCollector
	}
	p := &Pipeline{
		reader:           reader,
		validator:        validator,
		transformer:      transformer,
		exporter:         exporter,
		storage:          store,
		collector:        collector,
		progressInterval: DefaultProgressInterval,
		flushInterval:    DefaultFlushInterval,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// applyDefaults populates missing worker, batch, and timeout settings.
func applyDefaults(cfg *models.JobSpec) {
	if cfg.ValidationWorkers <= 0 {
		cfg.ValidationWorkers = DefaultWorkers
	}
	if cfg.TransformWorkers <= 0 {
		cfg.TransformWorkers = DefaultWorkers
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = DefaultTimeoutSeconds
	}
	if cfg.AggregationWorkers <= 0 {
		cfg.AggregationWorkers = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
}

// Execute runs the full pipeline for a job and persists its state (including live progress
// snapshots while it runs). onUpdate, if non-nil, is called when the job starts and finishes.
//
// Stage topology:
//
//	sources ─► raw ─► [validation pool] ─► valid ─► [transform pool] ─► transformed
//	        ─► [aggregator] ─► result ─► [export stage: storage + sinks]
//	every stage ─► errors ─► [error collector]      every stage ─► progress ─► [progress tracker]
func (p *Pipeline) Execute(parent context.Context, job *models.Job, onUpdate func(*models.Job)) {
	cfg := job.Spec
	applyDefaults(&cfg)
	job.Spec = cfg

	ctx, cancel := CreateJobContext(parent, cfg.TimeoutSeconds)
	defer cancel()

	slog.Info("pipeline job started", "job_id", job.ID, "sources_count", len(cfg.Sources))
	p.collector.IncJobsTotal()
	now := time.Now().UTC()
	job.Status = models.StatusRunning
	job.Metrics.StartedAt = now
	job.Metrics.StageLatency = make(map[string]time.Duration)
	job.UpdatedAt = now
	p.persist(job)
	notify(onUpdate, job)

	session, err := p.exporter.Open(job.ID, cfg.Exports)
	if err != nil {
		p.finalizeJob(ctx, parent, job, nil, err, 0)
		notify(onUpdate, job)
		return
	}

	// Snapshot base: copied before any goroutine starts, so the tracker never reads fields the
	// coordinator goroutine mutates later (ResultsPath, Artifacts, Aggregates, Status).
	base := *job
	snapshot := func(m models.Metrics) {
		s := base
		s.Metrics = m
		s.UpdatedAt = time.Now().UTC()
		p.persist(&s)
	}

	channels := NewChannels(cfg.BufferSize)
	progressWG := startProgressTracker(channels.Progress, job, p.collector, p.progressInterval, snapshot)
	errWG := startErrorCollector(ctx, channels.Errors, job.ID, p.storage, channels.Progress, p.flushInterval)

	ingestFailures := p.startIngestion(ctx, cfg, channels.Raw, channels.Progress, channels.Errors)

	validatorWG := startValidationWorkers(ctx, cfg.ValidationWorkers, p.validator, cfg.Rules, channels.Raw, channels.Valid, channels.Errors, channels.Progress)
	go func() {
		validatorWG.Wait()
		close(channels.Valid)
	}()

	transformWG := startTransformationWorkers(ctx, cfg.TransformWorkers, p.transformer, cfg.Rules, channels.Valid, channels.Transformed, channels.Errors, channels.Progress)
	go func() {
		transformWG.Wait()
		close(channels.Transformed)
	}()

	aggregates, exportErr := p.runAggregationAndExport(ctx, job.ID, cfg, session, channels)

	// Publish exports only for runs that finished normally; cancelled or timed-out runs keep
	// their persisted records (queryable via the API) but never publish partial files.
	switch {
	case exportErr != nil || ctx.Err() != nil:
		session.Abort()
	default:
		start := time.Now()
		exportErr = session.Close(aggregates)
		recordStageLatency(channels.Progress, "export_finalize", time.Since(start))
		if exportErr != nil {
			session.Abort()
		} else {
			job.ResultsPath = session.Primary()
			job.Artifacts = session.Artifacts()
		}
	}
	if exportErr != nil {
		slog.Error("pipeline export failed", "job_id", job.ID, "error", exportErr)
		channels.Errors <- models.RecordError{Stage: "export", Message: exportErr.Error(), At: time.Now()}
	}

	close(channels.Errors)
	errWG.Wait()

	if cfg.DLQPath != "" {
		written, err := exportDeadLetterRecords(context.WithoutCancel(ctx), cfg.DLQPath, job.ID, p.storage)
		if err != nil {
			slog.Error("dead-letter export failed", "job_id", job.ID, "error", err)
		} else if written {
			job.Artifacts = append(job.Artifacts, cfg.DLQPath)
		}
	}

	close(channels.Progress)
	progressWG.Wait()

	p.finalizeJob(ctx, parent, job, aggregates, exportErr, int(ingestFailures.Load()))
	slog.Info("pipeline job finished",
		"job_id", job.ID,
		"status", job.Status,
		"processed", job.Metrics.Processed,
		"rejected", job.Metrics.Rejected,
		"errors", job.Metrics.Errors,
		"duration", job.Metrics.EndedAt.Sub(job.Metrics.StartedAt).String(),
	)
	notify(onUpdate, job)
}

// notify triggers the onUpdate callback if provided by the caller.
func notify(onUpdate func(*models.Job), job *models.Job) {
	if onUpdate != nil {
		onUpdate(job)
	}
}

// persist saves job state using a context that survives job cancellation.
func (p *Pipeline) persist(job *models.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.storage.SaveJob(ctx, job); err != nil {
		slog.Error("failed persisting job state", "job_id", job.ID, "error", err)
	}
}

// timeoutError describes a job that exceeded its configured timeout.
func timeoutError(seconds int) error {
	return fmt.Errorf("job exceeded timeout of %ds", seconds)
}
