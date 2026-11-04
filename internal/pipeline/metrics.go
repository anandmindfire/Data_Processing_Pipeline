package pipeline

import (
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// latencyFlushEvery bounds how many operations a worker accumulates before reporting latency.
const latencyFlushEvery = 256

// recordStageLatency sends a single latency observation to the progress channel.
func recordStageLatency(progressCh chan<- progressMsg, stage string, d time.Duration) {
	progressCh <- progressMsg{kind: progressLatency, stage: stage, duration: d, count: 1}
}

// latencyAccumulator pre-aggregates per-record latencies inside one worker goroutine so the
// progress channel carries one message per latencyFlushEvery records instead of one per record.
type latencyAccumulator struct {
	stage      string
	progressCh chan<- progressMsg
	total      time.Duration
	n          int64
}

func newLatencyAccumulator(progressCh chan<- progressMsg, stage string) *latencyAccumulator {
	return &latencyAccumulator{stage: stage, progressCh: progressCh}
}

func (l *latencyAccumulator) add(d time.Duration) {
	l.total += d
	l.n++
	if l.n >= latencyFlushEvery {
		l.flush()
	}
}

func (l *latencyAccumulator) flush() {
	if l.n == 0 {
		return
	}
	l.progressCh <- progressMsg{kind: progressLatency, stage: l.stage, duration: l.total, count: l.n}
	l.total, l.n = 0, 0
}

// updateThroughput computes the throughput of processed records per second.
func updateThroughput(m *models.Metrics, startedAt, endedAt time.Time) {
	elapsed := endedAt.Sub(startedAt).Seconds()
	if elapsed > 0 {
		m.RecordsPerSecond = float64(m.Processed) / elapsed
	}
}

// publishCollectorMetrics publishes final job telemetry to the Prometheus collector.
func publishCollectorMetrics(collector *metrics.Collector, m models.Metrics) {
	if collector == nil {
		return
	}
	collector.AddRecordsProcessed(m.Processed)
	collector.AddRecordsRejected(m.Rejected)
	collector.AddRecordsExported(m.Exported)
	collector.AddErrors(m.Errors)
}
