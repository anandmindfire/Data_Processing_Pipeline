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
			recordStageLatency(progressCh, "ingestion", time.Since(start))
		}()
	}

	go func() {
		sourceWG.Wait()
		close(rawCh)
	}()

	return &ingestFailures
}

// runAggregationAndExport runs the fan-in aggregation goroutine and the export goroutine.
//
// The export goroutine batches records and, per batch, persists them to the repository and
// streams them to every export sink. It returns the first storage or sink error; after an error
// it keeps draining so upstream stages can finish.
func (p *Pipeline) runAggregationAndExport(
	ctx context.Context,
	jobID string,
	batchSize int,
	agg aggregation.Aggregator,
	session export.Session,
	ch *PipelineChannels,
) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(ch.Result)
		lat := newLatencyAccumulator(ch.Progress, "aggregation")
		defer lat.flush()
		for item := range ch.Transformed {
			start := time.Now()
			agg.Process(item.record)
			lat.add(time.Since(start))

			select {
			case ch.Result <- item.record:
			case <-ctx.Done():
				return
			}
		}
	}()

	var exportErr error
	go func() {
		defer wg.Done()
		exportErr = p.exportStage(ctx, jobID, batchSize, session, ch.Result, ch.Progress)
	}()

	wg.Wait()
	return exportErr
}

// exportStage batches incoming records, persists them to storage, and writes them to export sinks.
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
		job.Error = timeoutError(job.Config.TimeoutSeconds).Error()
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

	now := time.Now()
	job.Metrics.EndedAt = &now
	updateThroughput(&job.Metrics, job.Metrics.StartedAt, now)
	job.UpdatedAt = now

	publishCollectorMetrics(p.collector, job.Metrics)
	p.persist(job)
}
