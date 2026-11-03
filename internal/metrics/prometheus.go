package metrics

import (
	"fmt"
	"strings"
)

// FormatPrometheus formats the current metrics snapshot into Prometheus exposition format.
func (c *Collector) FormatPrometheus() string {
	s := c.GetSnapshot()

	var sb strings.Builder
	sb.WriteString("# HELP pipeline_jobs_total Total pipeline jobs submitted.\n")
	sb.WriteString("# TYPE pipeline_jobs_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_jobs_total %d\n", s.JobsTotal))

	sb.WriteString("# HELP pipeline_jobs_completed_total Total pipeline jobs successfully completed.\n")
	sb.WriteString("# TYPE pipeline_jobs_completed_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_jobs_completed_total %d\n", s.JobsCompleted))

	sb.WriteString("# HELP pipeline_jobs_failed_total Total pipeline jobs that failed.\n")
	sb.WriteString("# TYPE pipeline_jobs_failed_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_jobs_failed_total %d\n", s.JobsFailed))

	sb.WriteString("# HELP pipeline_jobs_cancelled_total Total pipeline jobs cancelled by user or timeout.\n")
	sb.WriteString("# TYPE pipeline_jobs_cancelled_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_jobs_cancelled_total %d\n", s.JobsCancelled))

	sb.WriteString("# HELP pipeline_jobs_active Current number of actively running pipeline jobs.\n")
	sb.WriteString("# TYPE pipeline_jobs_active gauge\n")
	sb.WriteString(fmt.Sprintf("pipeline_jobs_active %d\n", s.ActiveJobs))

	sb.WriteString("# HELP pipeline_records_processed_total Total records successfully processed across all jobs.\n")
	sb.WriteString("# TYPE pipeline_records_processed_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_records_processed_total %d\n", s.RecordsProcessedTotal))

	sb.WriteString("# HELP pipeline_records_rejected_total Total records rejected during validation or transformation.\n")
	sb.WriteString("# TYPE pipeline_records_rejected_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_records_rejected_total %d\n", s.RecordsRejectedTotal))

	sb.WriteString("# HELP pipeline_records_exported_total Total records successfully written to export sinks.\n")
	sb.WriteString("# TYPE pipeline_records_exported_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_records_exported_total %d\n", s.RecordsExportedTotal))

	sb.WriteString("# HELP pipeline_errors_total Total errors encountered across all stages.\n")
	sb.WriteString("# TYPE pipeline_errors_total counter\n")
	sb.WriteString(fmt.Sprintf("pipeline_errors_total %d\n", s.ErrorsTotal))

	for stage, avgSec := range s.StageAvgLatency {
		sb.WriteString(fmt.Sprintf("pipeline_stage_duration_seconds{stage=\"%s\"} %f\n", stage, avgSec))
	}

	return sb.String()
}

