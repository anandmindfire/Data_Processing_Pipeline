package metrics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/buildinfo"
)

// FormatPrometheus renders the metrics in the Prometheus text exposition format (version 0.0.4).
//
// Throughput is exposed as counters; use rate(pipeline_records_processed_total[5m]) for records
// per second. Stage latency is a summary (_sum/_count), so average latency per stage is
// rate(pipeline_stage_duration_seconds_sum[5m]) / rate(pipeline_stage_duration_seconds_count[5m]).
func (c *Collector) FormatPrometheus() string {
	s := c.GetSnapshot()
	var sb strings.Builder

	metric := func(name, typ, help string, value any) {
		fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, value)
	}

	info := buildinfo.Get()
	fmt.Fprintf(&sb, "# HELP pipeline_build_info Build metadata of the running binary.\n# TYPE pipeline_build_info gauge\n")
	fmt.Fprintf(&sb, "pipeline_build_info{version=%q,commit=%q,goversion=%q} 1\n", info.Version, info.Commit, info.GoVersion)

	metric("pipeline_jobs_total", "counter", "Total pipeline jobs started.", s.JobsTotal)
	metric("pipeline_jobs_completed_total", "counter", "Total pipeline jobs successfully completed.", s.JobsCompleted)
	metric("pipeline_jobs_failed_total", "counter", "Total pipeline jobs that failed.", s.JobsFailed)
	metric("pipeline_jobs_cancelled_total", "counter", "Total pipeline jobs cancelled by user or shutdown.", s.JobsCancelled)
	metric("pipeline_jobs_active", "gauge", "Current number of running pipeline jobs.", s.ActiveJobs)
	metric("pipeline_records_processed_total", "counter", "Total records successfully processed across all jobs.", s.RecordsProcessedTotal)
	metric("pipeline_records_rejected_total", "counter", "Total records rejected during validation or transformation.", s.RecordsRejectedTotal)
	metric("pipeline_records_exported_total", "counter", "Total records written to export sinks.", s.RecordsExportedTotal)
	metric("pipeline_errors_total", "counter", "Total errors encountered across all stages.", s.ErrorsTotal)

	stages := make([]string, 0, len(s.StageLatencyCount))
	for stage := range s.StageLatencyCount {
		stages = append(stages, stage)
	}
	sort.Strings(stages)
	sb.WriteString("# HELP pipeline_stage_duration_seconds Time spent per pipeline stage operation.\n")
	sb.WriteString("# TYPE pipeline_stage_duration_seconds summary\n")
	for _, stage := range stages {
		fmt.Fprintf(&sb, "pipeline_stage_duration_seconds_sum{stage=%q} %g\n", stage, s.StageLatencySum[stage])
		fmt.Fprintf(&sb, "pipeline_stage_duration_seconds_count{stage=%q} %d\n", stage, s.StageLatencyCount[stage])
	}
	return sb.String()
}
