package pipeline

import (
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestProgressTrackerAccumulation(t *testing.T) {
	ch := make(chan progressMsg, 10)
	job := &models.Job{ID: "job-progress-test"}
	collector := metrics.NewCollector()

	wg := startProgressTracker(ch, job, collector)

	ch <- progressMsg{kind: progressDiscovered, count: 100}
	ch <- progressMsg{kind: progressProcessed, count: 80}
	ch <- progressMsg{kind: progressRejected, count: 20}
	ch <- progressMsg{kind: progressExported, count: 80}
	ch <- progressMsg{kind: progressError, count: 5}
	ch <- progressMsg{kind: progressLatency, stage: "validation", duration: 50 * time.Millisecond}
	ch <- progressMsg{kind: progressLatency, stage: "validation", duration: 25 * time.Millisecond}
	ch <- progressMsg{kind: progressLatency, stage: "transform", duration: 10 * time.Millisecond}

	close(ch)
	wg.Wait()

	if job.Metrics.Discovered != 100 {
		t.Fatalf("expected 100 discovered, got %d", job.Metrics.Discovered)
	}
	if job.Metrics.Processed != 80 || job.Metrics.Valid != 80 {
		t.Fatalf("expected 80 processed/valid, got %d/%d", job.Metrics.Processed, job.Metrics.Valid)
	}
	if job.Metrics.Rejected != 20 {
		t.Fatalf("expected 20 rejected, got %d", job.Metrics.Rejected)
	}
	if job.Metrics.Exported != 80 {
		t.Fatalf("expected 80 exported, got %d", job.Metrics.Exported)
	}
	if job.Metrics.Errors != 5 {
		t.Fatalf("expected 5 errors, got %d", job.Metrics.Errors)
	}
	if job.Metrics.StageLatency["validation"] != 75*time.Millisecond {
		t.Fatalf("expected 75ms validation latency, got %v", job.Metrics.StageLatency["validation"])
	}
	if job.Metrics.StageLatency["transform"] != 10*time.Millisecond {
		t.Fatalf("expected 10ms transform latency, got %v", job.Metrics.StageLatency["transform"])
	}
}

func TestProgressTrackerNilJob(t *testing.T) {
	ch := make(chan progressMsg, 2)
	wg := startProgressTracker(ch, nil, nil)

	ch <- progressMsg{kind: progressDiscovered, count: 10}
	ch <- progressMsg{kind: progressProcessed, count: 10}
	close(ch)

	// Wait should complete without any nil pointer panic
	wg.Wait()
}

func TestProgressTrackerNilStageLatencyMap(t *testing.T) {
	ch := make(chan progressMsg, 2)
	job := &models.Job{ID: "nil-map-test"}
	// Explicitly ensure StageLatency map is nil
	job.Metrics.StageLatency = nil

	wg := startProgressTracker(ch, job, nil)
	ch <- progressMsg{kind: progressLatency, stage: "ingestion", duration: 15 * time.Millisecond}
	close(ch)

	// Wait should complete and allocate map without panic
	wg.Wait()

	if job.Metrics.StageLatency["ingestion"] != 15*time.Millisecond {
		t.Fatalf("expected 15ms latency, got %v", job.Metrics.StageLatency["ingestion"])
	}
}
