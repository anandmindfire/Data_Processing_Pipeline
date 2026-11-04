package response

import (
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// PipelineResponse represents the serialized job entity returned to clients.
type PipelineResponse struct {
	ID          string           `json:"id"`
	Status      models.JobStatus `json:"status"`
	Config      models.Config    `json:"config"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Metrics     models.Metrics   `json:"metrics"`
	ResultsPath string           `json:"results_path,omitempty"`
	Artifacts   []string         `json:"artifacts,omitempty"`
	Aggregates  map[string]any   `json:"aggregates,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// FromJob maps a domain models.Job to PipelineResponse DTO.
func FromJob(j *models.Job) PipelineResponse {
	if j == nil {
		return PipelineResponse{}
	}
	return PipelineResponse{
		ID:          j.ID,
		Status:      j.Status,
		Config:      j.Config,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
		Metrics:     j.Metrics,
		ResultsPath: j.ResultsPath,
		Artifacts:   j.Artifacts,
		Aggregates:  j.Aggregates,
		Error:       j.Error,
	}
}
