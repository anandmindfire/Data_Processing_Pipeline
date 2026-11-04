package pipeline

import (
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

type progressMsg struct {
	kind     progressKind
	count    int64
	stage    string
	duration time.Duration
}

// startProgressTracker starts a background goroutine consuming progress updates.
func startProgressTracker(progressCh <-chan progressMsg, job *models.Job, collector *metrics.Collector) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if job == nil {
			// Drain channel safely if job is nil to prevent blocking upstream pipeline workers
			for range progressCh {
			}
			return
		}

		if job.Metrics.StageLatency == nil {
			job.Metrics.StageLatency = make(map[string]time.Duration)
		}

		for msg := range progressCh {
			applyProgressMsg(job, collector, msg)
		}
	}()

	return &wg
}

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
			collector.ObserveStageLatency(msg.stage, msg.duration)
		}
	}
}
