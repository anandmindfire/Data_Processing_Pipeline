package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/aggregation"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// startIngestion launches one reader goroutine per source (fan-out) and closes rawCh when all finish.
func (p *Pipeline) startIngestion(
	ctx context.Context,
	cfg models.JobSpec,
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
			recordStageLatency(progressCh, "ingestion", time.Since(start))
		}()
	}

	go func() {
		sourceWG.Wait()
		close(rawCh)
	}()

	return &ingestFailures
}

// runAggregationAndExport runs the aggregation stage and the export goroutine and returns the
// merged aggregates together with the first export error.
//
// With workers == 1 a single goroutine aggregates (pure fan-in). With workers > 1 each worker
// owns a partial aggregator (fan-out, no locks); when a worker finishes it sends its partial on
// partialCh and one merger goroutine folds the partials into the final result (fan-in). Every
// worker also forwards records to the export stage through ch.Result.
func (p *Pipeline) runAggregationAndExport(
	ctx context.Context,
	jobID string,
	spec models.JobSpec,
	session export.Session,
	ch *Channels,
) (map[string]any, error) {
	workers := max(spec.AggregationWorkers, 1)
	partialCh := make(chan aggregation.Aggregator, workers)

	var aggWG sync.WaitGroup
	for n := 0; n < workers; n++ {
		aggWG.Add(1)
		go func() {
			defer aggWG.Done()
			partial := aggregation.NewAggregator(spec.Aggregations)
			defer func() { partialCh <- partial }()
			lat := newLatencyAccumulator(ch.Progress, "aggregation")
			defer lat.flush()
			for item := range ch.Transformed {
				rec := item.record
				if spec.IncludeSourceField {
					rec["_source"] = item.item.Source // rec is the transformer's private copy
				}
				start := time.Now()
				partial.Process(rec)
				lat.add(time.Since(start))

				select {
				case ch.Result <- rec:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		aggWG.Wait()
		close(ch.Result)
		close(partialCh)
	}()

	final := aggregation.NewAggregator(spec.Aggregations)
	mergeDone := make(chan struct{})
	go func() {
		defer close(mergeDone)
		for partial := range partialCh {
			final.Merge(partial)
		}
	}()

	exportErr := p.exportStage(ctx, jobID, spec.BatchSize, session, ch.Result, ch.Progress)
	<-mergeDone
	return final.Finalize(), exportErr
}

func (p *Pipeline) exportStage(
	ctx context.Context,
	jobID string,
	batchSize int,
	session export.Session,
	resultCh <-chan models.Record,
	progressCh chan<- progressMsg,
) error {
	storeCtx := context.WithoutCancel(ctx)
	batch := make([]models.Record, 0, batchSize)
	var firstErr error

	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		if err := p.storage.SaveRecords(storeCtx, jobID, batch); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("persist records: %w", err)
		}
		progressCh <- progressMsg{kind: progressProcessed, count: int64(len(batch))}
		if firstErr == nil {
			if err := session.Write(batch); err != nil {
				firstErr = err
			} else {
				progressCh <- progressMsg{kind: progressExported, count: int64(len(batch))}
			}
		}
		progressCh <- progressMsg{kind: progressLatency, stage: "export", duration: time.Since(start), count: int64(len(batch))}
		batch = batch[:0]
	}

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case rec, ok := <-resultCh:
			if !ok {
				flush()
				return firstErr
			}
			batch = append(batch, rec)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// finalizeJob determines the terminal status, records the reason, and persists the final state.
func (p *Pipeline) finalizeJob(
	ctx context.Context,
	parent context.Context,
	job *models.Job,
	aggregates map[string]any,
	exportErr error,
	ingestFailures int,
) {
	switch {
	case parent.Err() != nil:
		job.Status = models.StatusCancelled
		job.Error = "cancelled: " + context.Cause(parent).Error()
		p.collector.IncJobsCancelled()
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		job.Status = models.StatusFailed
		job.Error = timeoutError(job.Spec.TimeoutSeconds).Error()
		p.collector.IncJobsFailed()
	case exportErr != nil:
		job.Status = models.StatusFailed
		job.Error = "export failed: " + exportErr.Error()
		p.collector.IncJobsFailed()
	case job.Metrics.Discovered == 0 && ingestFailures > 0:
		job.Status = models.StatusFailed
		job.Error = fmt.Sprintf("all %d source(s) failed to ingest; see errors endpoint", ingestFailures)
		p.collector.IncJobsFailed()
	default:
		job.Status = models.StatusCompleted
		p.collector.IncJobsCompleted()
	}

	job.Aggregates = aggregates
	// Source credentials are only needed while the job runs; do not keep them at rest.
	job.Spec = job.Spec.WithRedactedHeaders()

	now := time.Now().UTC()
	job.Metrics.EndedAt = &now
	updateThroughput(&job.Metrics, job.Metrics.StartedAt, now)
	job.UpdatedAt = now

	publishCollectorMetrics(p.collector, job.Metrics)
	p.persist(job)
}
