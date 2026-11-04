package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/aggregation"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

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
			recordStageLatency(progressCh, "aggregation", time.Since(start))

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
	updateThroughput(&job.Metrics, job.Metrics.StartedAt, now)
	job.UpdatedAt = now

	publishCollectorMetrics(p.collector, job.Metrics)

	saveCtx, saveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer saveCancel()
	_ = p.storage.SaveJob(saveCtx, job)
}
