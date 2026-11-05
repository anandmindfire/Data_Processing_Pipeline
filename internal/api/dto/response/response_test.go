package response_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/response"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestFromJob(t *testing.T) {
	t.Run("nil job returns empty response", func(t *testing.T) {
		res := response.FromJob(nil)
		if res.ID != "" || res.Status != "" {
			t.Errorf("expected zero struct for nil job, got %+v", res)
		}
	})

	t.Run("valid job mapping", func(t *testing.T) {
		now := time.Now()
		job := &models.Job{
			ID:          "job-100",
			Status:      models.StatusCompleted,
			CreatedAt:   now,
			UpdatedAt:   now,
			ResultsPath: "exports/out.json",
			Artifacts:   []string{"exports/out.json"},
			Aggregates:  map[string]any{"avg": 42.0},
			Error:       "",
		}
		res := response.FromJob(job)
		if res.ID != "job-100" || res.Status != models.StatusCompleted || res.ResultsPath != "exports/out.json" {
			t.Fatalf("unexpected mapped fields: %+v", res)
		}

		b, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("failed marshaling PipelineResponse: %v", err)
		}
		if len(b) == 0 {
			t.Fatal("expected non-empty json")
		}
	})
}

func TestFromProgress(t *testing.T) {
	t.Run("nil progress returns empty response", func(t *testing.T) {
		res := response.FromProgress(nil)
		if res.JobID != "" {
			t.Errorf("expected zero struct for nil progress, got %+v", res)
		}
	})

	t.Run("valid progress mapping", func(t *testing.T) {
		now := time.Now()
		prog := &models.Progress{
			JobID:            "job-200",
			Status:           models.StatusRunning,
			Percent:          75.5,
			Processed:        150,
			Discovered:       200,
			Pending:          50,
			Valid:            140,
			Rejected:         10,
			ErrorCount:       10,
			StartedAt:        now,
			RecordsPerSecond: 1000.0,
			StageLatency:     map[string]time.Duration{"ingest": 10 * time.Millisecond},
		}
		res := response.FromProgress(prog)
		if res.JobID != "job-200" || res.Percent != 75.5 || res.Processed != 150 {
			t.Fatalf("unexpected mapped progress: %+v", res)
		}

		b, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("failed marshaling ProgressResponse: %v", err)
		}
		if len(b) == 0 {
			t.Fatal("expected non-empty json")
		}
	})
}

func TestErrorResponses(t *testing.T) {
	errResp := response.ErrorResponse{
		Error:   "bad input",
		Code:    "INVALID_ARGUMENT",
		Details: map[string]string{"field": "sources"},
	}
	b, err := json.Marshal(errResp)
	if err != nil || len(b) == 0 {
		t.Fatalf("failed marshaling ErrorResponse: %v", err)
	}

	listResp := response.ErrorsListResponse{
		JobID:  "job-300",
		Total:  1,
		Offset: 0,
		Limit:  10,
		Errors: []models.RecordError{
			{Stage: "validation", Message: "age must be positive", At: time.Now()},
		},
	}
	b2, err := json.Marshal(listResp)
	if err != nil || len(b2) == 0 {
		t.Fatalf("failed marshaling ErrorsListResponse: %v", err)
	}
}

func TestFromJobRedactsSourceHeaders(t *testing.T) {
	job := &models.Job{ID: "j", Spec: models.JobSpec{Sources: []models.Source{
		{Name: "api", URL: "https://x.test", Headers: map[string]string{"Authorization": "Bearer secret"}},
	}}}
	resp := response.FromJob(job)
	if got := resp.Config.Sources[0].Headers["Authorization"]; got != models.RedactedHeaderValue {
		t.Fatalf("expected redacted header, got %q", got)
	}
	if job.Spec.Sources[0].Headers["Authorization"] != "Bearer secret" {
		t.Fatal("redaction must not mutate the job used by the pipeline")
	}
}
