package domain

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// Type aliases to preserve backward compatibility while standardizing on internal/models.
type (
	Source       = models.Source
	Rule         = models.Rule
	Aggregation  = models.Aggregation
	ExportTarget = models.ExportTarget
	Config       = models.Config
	JobStatus    = models.JobStatus
	Record       = models.Record
	RecordError  = models.RecordError
	Metrics      = models.Metrics
	Job          = models.Job
	Progress     = models.Progress
)

const (
	Pending   = models.StatusPending
	Running   = models.StatusRunning
	Completed = models.StatusCompleted
	Failed    = models.StatusFailed
	Cancelled = models.StatusCancelled
)
