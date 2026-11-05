package pipeline

import (
	"maps"
	"sync"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
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

// progressMsg is a single progress update. For progressLatency, count is the number of
// operations whose combined duration is reported.
type progressMsg struct {
	kind     progressKind
	count    int64
	stage    string
	duration time.Duration
}

// startProgressTracker starts the single goroutine that owns job.Metrics while the pipeline runs.
//
// Every interval, if anything changed, it hands a copy of the metrics to snapshot so the job's
// live progress can be persisted and served by the API. snapshot may be nil.
func startProgressTracker(
	progressCh <-chan progressMsg,
	job *models.Job,
	collector *metrics.Collector,
	interval time.Duration,
	snapshot func(models.Metrics),
) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if job == nil {
			// Drain safely so upstream stages never block.
			for range progressCh {
			}
			return
		}
		if job.Metrics.StageLatency == nil {
			job.Metrics.StageLatency = make(map[string]time.Duration)
		}
		if interval <= 0 || snapshot == nil {
			for msg := range progressCh {
				applyProgressMsg(job, collector, msg)
			}
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		dirty := false
		for {
			select {
			case msg, ok := <-progressCh:
				if !ok {
					return
				}
				applyProgressMsg(job, collector, msg)
				dirty = true
			case <-ticker.C:
				if dirty {
					snapshot(metricsSnapshot(job.Metrics, time.Now()))
					dirty = false
				}
			}
		}
	}()

	return &wg
}

// metricsSnapshot deep-copies metrics and fills in the live throughput.
func metricsSnapshot(m models.Metrics, now time.Time) models.Metrics {
	m.StageLatency = maps.Clone(m.StageLatency)
	updateThroughput(&m, m.StartedAt, now)
	return m
}

// applyProgressMsg mutates the job metrics and reports updates to the Prometheus collector.
func applyProgressMsg(job *models.Job, collector *metrics.Collector, msg progressMsg) {
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
		if collector != nil {
			n := msg.count
			if n <= 0 {
				n = 1
			}
			collector.ObserveStageLatencyBatch(msg.stage, msg.duration, n)
		}
	}
}
