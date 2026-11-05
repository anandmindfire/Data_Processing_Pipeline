package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestMetricsCollectorAndPrometheus(t *testing.T) {
	c := NewCollector()
	c.IncJobsTotal()
	c.AddRecordsProcessed(10)
	c.AddRecordsRejected(2)
	c.AddErrors(3)
	c.ObserveStageLatency("validation", 15*time.Millisecond)

	s := c.GetSnapshot()
	if s.JobsTotal != 1 || s.ActiveJobs != 1 {
		t.Fatalf("unexpected jobs count: %+v", s)
	}
	if s.RecordsProcessedTotal != 10 {
		t.Fatalf("expected 10 records, got %d", s.RecordsProcessedTotal)
	}

	c.IncJobsCompleted()
	s = c.GetSnapshot()
	if s.JobsCompleted != 1 || s.ActiveJobs != 0 {
		t.Fatalf("expected 1 completed and 0 active, got %+v", s)
	}

	prom := c.FormatPrometheus()
	if !strings.Contains(prom, "pipeline_jobs_total 1") {
		t.Fatalf("missing metric in output: %s", prom)
	}
	if !strings.Contains(prom, "pipeline_records_processed_total 10") {
		t.Fatalf("missing processed metric: %s", prom)
	}
}

func TestPrometheusStageSummaryFormat(t *testing.T) {
	c := NewCollector()
	c.ObserveStageLatencyBatch("validation", 3*time.Second, 6)
	out := c.FormatPrometheus()
	for _, want := range []string{
		"# TYPE pipeline_stage_duration_seconds summary",
		`pipeline_stage_duration_seconds_sum{stage="validation"} 3`,
		`pipeline_stage_duration_seconds_count{stage="validation"} 6`,
		"# TYPE pipeline_build_info gauge",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}
