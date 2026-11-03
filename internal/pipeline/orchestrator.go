package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/aggregation"
	"github.com/anandkr47/data-processing-pipeline/internal/ingestion"
	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

// Execute runs the full pipeline for a job, publishing updates to storage.
func (p *Pipeline) Execute(parent context.Context, job *models.Job, onUpdate func(*models.Job)) {
	cfg := job.Config
	applyDefaults(&cfg)
	job.Config = cfg

	ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
	defer cancel()

	var stateMu sync.Mutex
	ingestFailures := 0

	job.Status = models.StatusRunning
	job.Metrics.StartedAt = time.Now()
	job.Metrics.StageLatency = make(map[string]time.Duration)
	_ = p.storage.SaveJob(ctx, job)
	if onUpdate != nil {
		onUpdate(job)
	}

	recordLatency := func(stage string, d time.Duration) {
		stateMu.Lock()
		job.Metrics.StageLatency[stage] += d
		stateMu.Unlock()
		p.collector.ObserveStageLatency(stage, d)
	}

	// Channel graph initialization (bounded channels enforce backpressure)
	rawCh := make(chan ingestion.Item, cfg.BufferSize)
	validCh := make(chan ingestion.Item, cfg.BufferSize)
	transformedCh := make(chan transformResult, cfg.BufferSize)
	errCh := make(chan models.RecordError, cfg.BufferSize)

	// 1. Ingestion stage
	p.runIngestion(ctx, cfg.Sources, cfg.MaxRecords, rawCh, errCh, job, &stateMu, &ingestFailures, recordLatency)

	// 2. Validation stage
	validatorWG := startValidationWorkers(ctx, cfg.ValidationWorkers, p.validator, cfg.Rules, rawCh, validCh, errCh, recordLatency)
	go func() {
		validatorWG.Wait()
		close(validCh)
	}()

	// 3. Transformation stage
	transformWG := startTransformationWorkers(ctx, cfg.TransformWorkers, p.transformer, cfg.Rules, validCh, transformedCh, errCh, recordLatency)
	go func() {
		transformWG.Wait()
		close(transformedCh)
	}()

	// 4. Aggregation & Error collection stages
	agg := aggregation.NewAggregator(cfg.Aggregations)
	p.consumeAndPersist(ctx, agg, transformedCh, errCh, job, &stateMu, recordLatency)

	// Finalize status, export, and telemetry
	p.completeJob(ctx, parent, job, agg, cfg.Exports, ingestFailures, &stateMu)
	if onUpdate != nil {
		onUpdate(job)
	}
}

func (p *Pipeline) runIngestion(
	ctx context.Context,
	sources []models.Source,
	max int,
	rawCh chan<- ingestion.Item,
	errCh chan<- models.RecordError,
	job *models.Job,
	mu *sync.Mutex,
	ingestFailures *int,
	recordLatency func(string, time.Duration),
) {
	var wg sync.WaitGroup
	for _, src := range sources {
		s := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			err := p.reader.Read(ctx, s, max, rawCh, func() {
				mu.Lock()
				job.Metrics.Discovered++
				mu.Unlock()
			})
			if err != nil && ctx.Err() == nil {
				mu.Lock()
				*ingestFailures++
				mu.Unlock()
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
		wg.Wait()
		close(rawCh)
	}()
}

func (p *Pipeline) consumeAndPersist(
	ctx context.Context,
	agg aggregation.Aggregator,
	transformedCh <-chan transformResult,
	errCh chan models.RecordError,
	job *models.Job,
	mu *sync.Mutex,
	recordLatency func(string, time.Duration),
) {
	var aggWG sync.WaitGroup
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		for item := range transformedCh {
			start := time.Now()
			_ = p.storage.SaveRecord(ctx, job.ID, item.record)
			mu.Lock()
			job.Metrics.Processed++
			job.Metrics.Valid++
			mu.Unlock()

			agg.Process(item.record)
			recordLatency("aggregation", time.Since(start))
		}
	}()

	var errWG sync.WaitGroup
	errWG.Add(1)
	go func() {
		defer errWG.Done()
		for er := range errCh {
			_ = p.storage.AddError(ctx, job.ID, er)
			mu.Lock()
			job.Metrics.Errors++
			if er.Stage == "validation" || er.Stage == "transform" {
				job.Metrics.Rejected++
			}
			mu.Unlock()
		}
	}()

	aggWG.Wait()
	close(errCh)
	errWG.Wait()
}

func (p *Pipeline) completeJob(
	ctx context.Context,
	parent context.Context,
	job *models.Job,
	agg aggregation.Aggregator,
	exports []models.ExportTarget,
	ingestFailures int,
	mu *sync.Mutex,
) {
	if parent.Err() != nil {
		job.Status = models.StatusCancelled
		p.collector.IncJobsCancelled()
	} else if ctx.Err() != nil {
		job.Status = models.StatusFailed
		p.collector.IncJobsFailed()
	} else {
		job.Status = models.StatusCompleted
	}

	mu.Lock()
	if job.Metrics.Discovered == 0 && ingestFailures > 0 {
		job.Status = models.StatusFailed
		p.collector.IncJobsFailed()
	} else if job.Status == models.StatusCompleted {
		p.collector.IncJobsCompleted()
	}
	mu.Unlock()

	job.Aggregates = agg.Finalize()

	// Export stage
	records, _ := p.storage.GetRecords(ctx, job.ID)
	primaryPath, expErr := p.exporter.Export(records, exports, job.ID)
	if expErr != nil {
		job.Status = models.StatusFailed
		job.Metrics.Errors++
		_ = p.storage.AddError(ctx, job.ID, models.RecordError{
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

	p.collector.AddRecordsProcessed(job.Metrics.Processed)
	p.collector.AddRecordsRejected(job.Metrics.Rejected)
	p.collector.AddRecordsExported(job.Metrics.Exported)
	p.collector.AddErrors(job.Metrics.Errors)

	_ = p.storage.SaveJob(ctx, job)
}

