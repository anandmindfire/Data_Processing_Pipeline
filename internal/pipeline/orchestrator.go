package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/aggregation"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

type progressKind int

const (
	progressDiscovered progressKind = iota
	progressProcessed
	progressRejected
	progressExported
	progressError
	progressLatency
)

type progressMsg struct {
	kind     progressKind
	count    int64
	stage    string
	duration time.Duration
}

// Execute runs the full pipeline for a job, publishing updates to storage.
// It implements the full multi-stage channel topology:
// recordsCh -> validatedCh -> transformedCh -> resultCh
// with side channels: errorCh and progressCh.
func (p *Pipeline) Execute(parent context.Context, job *models.Job, onUpdate func(*models.Job)) {
	cfg := job.Config
	applyDefaults(&cfg)
	job.Config = cfg

	ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
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
	rawCh := make(chan ingestion.Item, cfg.BufferSize)
	validCh := make(chan ingestion.Item, cfg.BufferSize)
	transformedCh := make(chan transformResult, cfg.BufferSize)
	resultCh := make(chan models.Record, cfg.BufferSize)
	errCh := make(chan models.RecordError, cfg.BufferSize)
	progressCh := make(chan progressMsg, cfg.BufferSize*2)

	// 2. Start side-channel listener: Progress Tracking goroutine (sole owner of job.Metrics)
	var progressWG sync.WaitGroup
	progressWG.Add(1)
	go func() {
		defer progressWG.Done()
		for msg := range progressCh {
			switch msg.kind {
			case progressDiscovered:
				job.Metrics.Discovered += msg.count
			case progressProcessed:
				job.Metrics.Processed += msg.count
				job.Metrics.Valid += msg.count
			case progressRejected:
				job.Metrics.Rejected += msg.count
			case progressExported:
				job.Metrics.Exported += msg.count
			case progressError:
				job.Metrics.Errors += msg.count
			case progressLatency:
				job.Metrics.StageLatency[msg.stage] += msg.duration
				p.collector.ObserveStageLatency(msg.stage, msg.duration)
			}
		}
	}()

	// 3. Start side-channel listener: Centralized Error Collection goroutine
	var errWG sync.WaitGroup
	errWG.Add(1)
	go func() {
		defer errWG.Done()
		for er := range errCh {
			_ = p.storage.AddError(ctx, job.ID, er)
			progressCh <- progressMsg{kind: progressError, count: 1}
			if er.Stage == "validation" || er.Stage == "transform" {
				progressCh <- progressMsg{kind: progressRejected, count: 1}
			}
		}
	}()

	// 4. Ingestion Stage: goroutine per source -> sends to rawCh (recordsCh)
	ingestFailures := p.startIngestion(ctx, cfg, rawCh, progressCh, errCh)

	// 5. Validation Stage: worker pool fan-out -> sends to validCh
	recordLatency := func(stage string, d time.Duration) {
		progressCh <- progressMsg{kind: progressLatency, stage: stage, duration: d}
	}
	validatorWG := startValidationWorkers(ctx, cfg.ValidationWorkers, p.validator, cfg.Rules, rawCh, validCh, errCh, recordLatency)
	go func() {
		validatorWG.Wait()
		close(validCh)
	}()

	// 6. Transformation Stage: worker pool fan-out -> sends to transformedCh
	transformWG := startTransformationWorkers(ctx, cfg.TransformWorkers, p.transformer, cfg.Rules, validCh, transformedCh, errCh, recordLatency)
	go func() {
		transformWG.Wait()
		close(transformedCh)
	}()

	// 7. Aggregation Stage: single fan-in goroutine -> accumulates & forwards to resultCh
	agg := aggregation.NewAggregator(cfg.Aggregations)
	p.runAggregationAndExport(ctx, job, agg, transformedCh, resultCh, progressCh)

	// Perform file exports
	exportStart := time.Now()
	exportedRecords, _ := p.storage.GetRecords(ctx, job.ID)
	primaryPath, expErr := p.exporter.Export(exportedRecords, cfg.Exports, job.ID)
	progressCh <- progressMsg{kind: progressLatency, stage: "export", duration: time.Since(exportStart)}
	if expErr != nil {
		slog.Error("pipeline export failed", "job_id", job.ID, "error", expErr)
		errCh <- models.RecordError{Stage: "export", Message: expErr.Error(), At: time.Now()}
	} else {
		job.ResultsPath = primaryPath
		progressCh <- progressMsg{kind: progressExported, count: int64(len(exportedRecords))}
	}

	// Close errorCh and progressCh after all stages are complete
	close(errCh)
	errWG.Wait()

	if cfg.DeadLetterPath != "" {
		p.exportDeadLetterRecords(ctx, cfg.DeadLetterPath, job.ID)
	}

	close(progressCh)
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

func (p *Pipeline) startIngestion(
	ctx context.Context,
	cfg models.Config,
	rawCh chan<- ingestion.Item,
	progressCh chan<- progressMsg,
	errCh chan<- models.RecordError,
) *atomic.Int64 {
	var ingestFailures atomic.Int64
	var sourceWG sync.WaitGroup

	for _, src := range cfg.Sources {
		s := src
		sourceWG.Add(1)
		go func() {
			defer sourceWG.Done()
			start := time.Now()
			err := p.reader.Read(ctx, s, cfg.MaxRecords, rawCh, func() {
				progressCh <- progressMsg{kind: progressDiscovered, count: 1}
			})
			if err != nil && ctx.Err() == nil {
				ingestFailures.Add(1)
				slog.Warn("source ingestion failed", "source", s.Name, "error", err)
				errCh <- models.RecordError{
					Source:  s.Name,
					Stage:   "ingest",
					Message: err.Error(),
					At:      time.Now(),
				}
			}
			progressCh <- progressMsg{kind: progressLatency, stage: "ingestion", duration: time.Since(start)}
		}()
	}

	go func() {
		sourceWG.Wait()
		close(rawCh)
	}()

	return &ingestFailures
}

func (p *Pipeline) runAggregationAndExport(
	ctx context.Context,
	job *models.Job,
	agg aggregation.Aggregator,
	transformedCh <-chan transformResult,
	resultCh chan models.Record,
	progressCh chan<- progressMsg,
) {
	var aggWG sync.WaitGroup
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		defer close(resultCh)
		for item := range transformedCh {
			start := time.Now()
			agg.Process(item.record)
			progressCh <- progressMsg{kind: progressLatency, stage: "aggregation", duration: time.Since(start)}

			select {
			case resultCh <- item.record:
			case <-ctx.Done():
				return
			}
		}
	}()

	var exportWG sync.WaitGroup
	exportWG.Add(1)
	go func() {
		defer exportWG.Done()
		for rec := range resultCh {
			_ = p.storage.SaveRecord(ctx, job.ID, rec)
			progressCh <- progressMsg{kind: progressProcessed, count: 1}
		}
	}()

	aggWG.Wait()
	exportWG.Wait()
}

func (p *Pipeline) finalizeJob(
	ctx context.Context,
	parent context.Context,
	job *models.Job,
	agg aggregation.Aggregator,
	exportErr error,
	ingestFailures int,
) {
	switch {
	case parent.Err() != nil:
		job.Status = models.StatusCancelled
		p.collector.IncJobsCancelled()
	case ctx.Err() != nil || exportErr != nil || (job.Metrics.Discovered == 0 && ingestFailures > 0):
		job.Status = models.StatusFailed
		p.collector.IncJobsFailed()
	default:
		job.Status = models.StatusCompleted
		p.collector.IncJobsCompleted()
	}

	job.Aggregates = agg.Finalize()

	now := time.Now()
	job.Metrics.EndedAt = &now
	elapsed := now.Sub(job.Metrics.StartedAt).Seconds()
	if elapsed > 0 {
		job.Metrics.RecordsPerSecond = float64(job.Metrics.Processed) / elapsed
	}
	job.UpdatedAt = now

	p.collector.AddRecordsProcessed(job.Metrics.Processed)
	p.collector.AddRecordsRejected(job.Metrics.Rejected)
	p.collector.AddRecordsExported(job.Metrics.Exported)
	p.collector.AddErrors(job.Metrics.Errors)

	saveCtx, saveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer saveCancel()
	_ = p.storage.SaveJob(saveCtx, job)
}

func (p *Pipeline) exportDeadLetterRecords(ctx context.Context, path string, jobID string) {
	errs, err := p.storage.GetErrors(ctx, jobID)
	if err != nil || len(errs) == 0 {
		return
	}
	b, err := json.MarshalIndent(errs, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.WriteFile(path, b, 0644)
	slog.Info("dead-letter records exported", "job_id", jobID, "count", len(errs), "path", path)
}
