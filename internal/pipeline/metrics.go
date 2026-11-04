package pipeline

import (
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// recordStageLatency helper sends latency data to progress channel.
func recordStageLatency(progressCh chan<- progressMsg, stage string, d time.Duration) {
	progressCh <- progressMsg{kind: progressLatency, stage: stage, duration: d}
}

// updateThroughput computes the throughput of processed records per second.
func updateThroughput(m *models.Metrics, startedAt, endedAt time.Time) {
	elapsed := endedAt.Sub(startedAt).Seconds()
	if elapsed > 0 {
		m.RecordsPerSecond = float64(m.Processed) / elapsed
	}
}

// publishCollectorMetrics publishes final job telemetry to Prometheus collector.
func publishCollectorMetrics(collector *metrics.Collector, m models.Metrics) {
	if collector == nil {
		return
	}
	collector.AddRecordsProcessed(m.Processed)
	collector.AddRecordsRejected(m.Rejected)
	collector.AddRecordsExported(m.Exported)
	collector.AddErrors(m.Errors)
}
