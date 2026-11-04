package request

import "github.com/anandmindfire/data-processing-pipeline/internal/models"

// CreatePipelineRequest defines the incoming payload for initiating a pipeline job.
type CreatePipelineRequest struct {
	Sources           []models.Source       `json:"sources"`
	Rules             []models.Rule         `json:"rules,omitempty"`
	Aggregations      []models.Aggregation  `json:"aggregations,omitempty"`
	Exports           []models.ExportTarget `json:"exports"`
	ValidationWorkers int                   `json:"validation_workers,omitempty"`
	TransformWorkers  int                   `json:"transform_workers,omitempty"`
	BufferSize        int                   `json:"buffer_size,omitempty"`
	TimeoutSeconds    int                   `json:"timeout_seconds,omitempty"`
	BatchSize         int                   `json:"batch_size,omitempty"`
	MaxRecords        int                   `json:"max_records,omitempty"`
	DLQPath           string                `json:"dlq_path,omitempty"`
	// Deprecated: use dlq_path. Accepted as an alias for backward compatibility.
	DeadLetterPath string `json:"dead_letter_path,omitempty"`
}

// ToConfig converts CreatePipelineRequest into models.Config domain entity.
func (r CreatePipelineRequest) ToConfig() models.Config {
	dlq := r.DLQPath
	if dlq == "" {
		dlq = r.DeadLetterPath
	}
	return models.Config{
		Sources:           r.Sources,
		Rules:             r.Rules,
		Aggregations:      r.Aggregations,
		Exports:           r.Exports,
		ValidationWorkers: r.ValidationWorkers,
		TransformWorkers:  r.TransformWorkers,
		BufferSize:        r.BufferSize,
		TimeoutSeconds:    r.TimeoutSeconds,
		BatchSize:         r.BatchSize,
		MaxRecords:        r.MaxRecords,
		DLQPath:           dlq,
	}
}
