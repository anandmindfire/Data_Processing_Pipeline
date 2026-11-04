package response

import (
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// ProgressResponse represents real-time progress information for an executing job.
type ProgressResponse struct {
	JobID            string                   `json:"job_id"`
	Status           models.JobStatus         `json:"status"`
	Error            string                   `json:"error,omitempty"`
	Percent          float64                  `json:"percent_complete"`
	Processed        int64                    `json:"processed"`
	Discovered       int64                    `json:"discovered"`
	Pending          int64                    `json:"pending"`
	Valid            int64                    `json:"valid"`
	Rejected         int64                    `json:"rejected"`
	ErrorCount       int64                    `json:"error_count"`
	StartedAt        time.Time                `json:"started_at,omitempty"`
	EndedAt          *time.Time               `json:"ended_at,omitempty"`
	RecordsPerSecond float64                  `json:"records_per_second"`
	StageLatency     map[string]time.Duration `json:"stage_latency_ns"`
}

// FromProgress maps a models.Progress domain model to ProgressResponse.
func FromProgress(p *models.Progress) ProgressResponse {
	if p == nil {
		return ProgressResponse{}
	}
	return ProgressResponse{
		JobID:            p.JobID,
		Status:           p.Status,
		Error:            p.Error,
		Percent:          p.Percent,
		Processed:        p.Processed,
		Discovered:       p.Discovered,
		Pending:          p.Pending,
		Valid:            p.Valid,
		Rejected:         p.Rejected,
		ErrorCount:       p.ErrorCount,
		StartedAt:        p.StartedAt,
		EndedAt:          p.EndedAt,
		RecordsPerSecond: p.RecordsPerSecond,
		StageLatency:     p.StageLatency,
	}
}
