package pipeline

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/models"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline/stages"
	"github.com/anandkr47/data-processing-pipeline/internal/repository"
	"github.com/anandkr47/data-processing-pipeline/internal/sources"
)

// Executor runs an individual pipeline job through its stage lifecycle.
type Executor struct {
	Repo      repository.Repository
	Client    *http.Client
	ExportDir string
	Collector *metrics.Collector
}

// NewExecutor creates a new Executor.
func NewExecutor(repo repository.Repository, client *http.Client, exportDir string, col *metrics.Collector) *Executor {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if exportDir == "" {
		exportDir = "exports"
	}
	if col == nil {
		col = metrics.GlobalCollector
	}
	return &Executor{
		Repo:      repo,
		Client:    client,
		ExportDir: exportDir,
		Collector: col,
	}
}

// ApplyDefaults sets safe default parameters for unset config fields.
func ApplyDefaults(c *models.Config) {
	if c.ValidationWorkers <= 0 {
		c.ValidationWorkers = 4
	}
	if c.TransformWorkers <= 0 {
		c.TransformWorkers = 4
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 128
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 300
	}
}

// Execute runs the full pipeline for a job.
func (e *Executor) Execute(parent context.Context, job *models.Job, updateCallback func(*models.Job)) {
	cfg := job.Config
	ApplyDefaults(&cfg)
	job.Config = cfg

	ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
	defer cancel()

	var stateMu sync.Mutex
	ingestFailures := 0

	job.Status = models.StatusRunning
	job.Metrics.StartedAt = time.Now()
	job.Metrics.StageLatency = make(map[string]time.Duration)

	_ = e.Repo.SaveJob(ctx, job)
	if updateCallback != nil {
		updateCallback(job)
	}

	recordLatency := func(stage string, d time.Duration) {
		stateMu.Lock()
		job.Metrics.StageLatency[stage] += d
		stateMu.Unlock()
		e.Collector.ObserveStageLatency(stage, d)
	}

	raw := make(chan sources.Item, cfg.BufferSize)
	valid := make(chan sources.Item, cfg.BufferSize)
	transformed := make(chan transformResult, cfg.BufferSize)
	errCh := make(chan models.RecordError, cfg.BufferSize)

	// 1. Stage: Ingestion (goroutine per source)
	var sourceWG sync.WaitGroup
	for _, src := range cfg.Sources {
		s := src
		sourceWG.Add(1)
		go func() {
			defer sourceWG.Done()
			start := time.Now()
			err := sources.Read(ctx, s, e.Client, cfg.MaxRecords, raw, func() {
				stateMu.Lock()
				job.Metrics.Discovered++
				stateMu.Unlock()
			})
			if err != nil && ctx.Err() == nil {
				stateMu.Lock()
				ingestFailures++
				stateMu.Unlock()
				errCh <- models.RecordError{
					Source:  s.Name,
					Stage:   "ingest",
					Message: err.Error(),
					At:      time.Now(),
				}
			}
			recordLatency("ingestion", time.Since(start))
		}()
	}
	go func() {
		sourceWG.Wait()
		close(raw)
	}()

	// 2. Stage: Validation Workers
	validatorWG := startValidationWorkers(ctx, cfg.ValidationWorkers, cfg.Rules, raw, valid, errCh, recordLatency)
	go func() {
		validatorWG.Wait()
		close(valid)
	}()

	// 3. Stage: Transformation Workers
	transformWG := startTransformWorkers(ctx, cfg.TransformWorkers, cfg.Rules, valid, transformed, errCh, recordLatency)
	go func() {
		transformWG.Wait()
		close(transformed)
	}()

	// 4. Stage: Aggregation Engine (single fan-in collector)
	aggregator := stages.NewAggregator(cfg.Aggregations)
	aggDone := make(chan struct{})
	go func() {
		defer close(aggDone)
		for item := range transformed {
			start := time.Now()
			_ = e.Repo.SaveRecord(ctx, job.ID, item.record)
			stateMu.Lock()
			job.Metrics.Processed++
			job.Metrics.Valid++
			stateMu.Unlock()

			aggregator.Process(item.record)
			recordLatency("aggregation", time.Since(start))
		}
	}()

	// 5. Centralized Error Collection Worker
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		for er := range errCh {
			_ = e.Repo.AddError(ctx, job.ID, er)
			stateMu.Lock()
			job.Metrics.Errors++
			if er.Stage == "validation" || er.Stage == "transform" {
				job.Metrics.Rejected++
			}
			stateMu.Unlock()
		}
	}()

	<-aggDone
	close(errCh)
	<-errDone

	// Determine final job state
	if parent.Err() != nil {
		job.Status = models.StatusCancelled
		e.Collector.IncJobsCancelled()
	} else if ctx.Err() != nil {
		job.Status = models.StatusFailed
		e.Collector.IncJobsFailed()
	} else {
		job.Status = models.StatusCompleted
	}

	stateMu.Lock()
	allSourcesFailed := job.Metrics.Discovered == 0 && ingestFailures > 0
	stateMu.Unlock()
	if allSourcesFailed {
		job.Status = models.StatusFailed
		e.Collector.IncJobsFailed()
	} else if job.Status == models.StatusCompleted {
		e.Collector.IncJobsCompleted()
	}

	// Finalize aggregations
	job.Aggregates = aggregator.Finalize()

	// 6. Stage: Export Data
	exportStart := time.Now()
	recs, _ := e.Repo.GetRecords(ctx, job.ID)
	primaryPath, expErr := stages.Export(recs, cfg.Exports, e.ExportDir, job.ID)
	recordLatency("export", time.Since(exportStart))

	if expErr != nil {
		job.Status = models.StatusFailed
		job.Metrics.Errors++
		_ = e.Repo.AddError(ctx, job.ID, models.RecordError{
			Stage:   "export",
			Message: expErr.Error(),
			At:      time.Now(),
		})
	} else {
		job.ResultsPath = primaryPath
		job.Metrics.Exported = job.Metrics.Processed
	}

	now := time.Now()
	job.Metrics.EndedAt = &now
	elapsed := now.Sub(job.Metrics.StartedAt).Seconds()
	if elapsed > 0 {
		job.Metrics.RecordsPerSecond = float64(job.Metrics.Processed) / elapsed
	}
	job.UpdatedAt = now

	// Update telemetry totals
	e.Collector.AddRecordsProcessed(job.Metrics.Processed)
	e.Collector.AddRecordsRejected(job.Metrics.Rejected)
	e.Collector.AddRecordsExported(job.Metrics.Exported)
	e.Collector.AddErrors(job.Metrics.Errors)

	_ = e.Repo.SaveJob(ctx, job)
	if updateCallback != nil {
		updateCallback(job)
	}
}

