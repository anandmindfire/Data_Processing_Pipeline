package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Collector tracks service-level telemetry and pipeline metrics.
type Collector struct {
	mu                    sync.RWMutex
	jobsTotal             int64
	jobsCompleted         int64
	jobsFailed            int64
	jobsCancelled         int64
	recordsProcessedTotal int64
	recordsRejectedTotal  int64
	recordsExportedTotal  int64
	errorsTotal           int64
	activeJobs            int64
	stageDurationSum      map[string]time.Duration
	stageDurationCount    map[string]int64
}

// GlobalCollector is the singleton metrics instance.
var GlobalCollector = NewCollector()

// NewCollector initializes an empty Collector.
func NewCollector() *Collector {
	return &Collector{
		stageDurationSum:   make(map[string]time.Duration),
		stageDurationCount: make(map[string]int64),
	}
}

// IncJobsTotal increments the total submitted jobs count.
func (c *Collector) IncJobsTotal() {
	atomic.AddInt64(&c.jobsTotal, 1)
	atomic.AddInt64(&c.activeJobs, 1)
}

// IncJobsCompleted increments the completed jobs count.
func (c *Collector) IncJobsCompleted() {
	atomic.AddInt64(&c.jobsCompleted, 1)
	atomic.AddInt64(&c.activeJobs, -1)
}

// IncJobsFailed increments the failed jobs count.
func (c *Collector) IncJobsFailed() {
	atomic.AddInt64(&c.jobsFailed, 1)
	atomic.AddInt64(&c.activeJobs, -1)
}

// IncJobsCancelled increments the cancelled jobs count.
func (c *Collector) IncJobsCancelled() {
	atomic.AddInt64(&c.jobsCancelled, 1)
	atomic.AddInt64(&c.activeJobs, -1)
}

// AddRecordsProcessed increments the total processed records count.
func (c *Collector) AddRecordsProcessed(n int64) {
	atomic.AddInt64(&c.recordsProcessedTotal, n)
}

// AddRecordsRejected increments the total rejected records count.
func (c *Collector) AddRecordsRejected(n int64) {
	atomic.AddInt64(&c.recordsRejectedTotal, n)
}

// AddRecordsExported increments the total exported records count.
func (c *Collector) AddRecordsExported(n int64) {
	atomic.AddInt64(&c.recordsExportedTotal, n)
}

// AddErrors increments total error occurrences.
func (c *Collector) AddErrors(n int64) {
	atomic.AddInt64(&c.errorsTotal, n)
}

// ObserveStageLatency records latency for a specific stage.
func (c *Collector) ObserveStageLatency(stage string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageDurationSum[stage] += d
	c.stageDurationCount[stage]++
}

// ObserveStageLatencyBatch records the total latency of n operations for a stage.
// Workers pre-aggregate latencies to avoid one channel message per record.
func (c *Collector) ObserveStageLatencyBatch(stage string, total time.Duration, n int64) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageDurationSum[stage] += total
	c.stageDurationCount[stage] += n
}

// Snapshot captures current metrics values for export.
type Snapshot struct {
	JobsTotal             int64
	JobsCompleted         int64
	JobsFailed            int64
	JobsCancelled         int64
	ActiveJobs            int64
	RecordsProcessedTotal int64
	RecordsRejectedTotal  int64
	RecordsExportedTotal  int64
	ErrorsTotal           int64
	StageAvgLatency       map[string]float64 // in seconds
}

// GetSnapshot returns a consistent snapshot of metrics.
func (c *Collector) GetSnapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	avgLatency := make(map[string]float64, len(c.stageDurationCount))
	for stage, count := range c.stageDurationCount {
		if count > 0 {
			avgLatency[stage] = c.stageDurationSum[stage].Seconds() / float64(count)
		}
	}

	return Snapshot{
		JobsTotal:             atomic.LoadInt64(&c.jobsTotal),
		JobsCompleted:         atomic.LoadInt64(&c.jobsCompleted),
		JobsFailed:            atomic.LoadInt64(&c.jobsFailed),
		JobsCancelled:         atomic.LoadInt64(&c.jobsCancelled),
		ActiveJobs:            atomic.LoadInt64(&c.activeJobs),
		RecordsProcessedTotal: atomic.LoadInt64(&c.recordsProcessedTotal),
		RecordsRejectedTotal:  atomic.LoadInt64(&c.recordsRejectedTotal),
		RecordsExportedTotal:  atomic.LoadInt64(&c.recordsExportedTotal),
		ErrorsTotal:           atomic.LoadInt64(&c.errorsTotal),
		StageAvgLatency:       avgLatency,
	}
}
