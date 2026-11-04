package request_test

import (
	"encoding/json"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestCreatePipelineRequestToConfig(t *testing.T) {
	req := request.CreatePipelineRequest{
		Sources:           []models.Source{{Type: "csv", Name: "source-1"}},
		Rules:             []models.Rule{{Field: "age", Type: "number"}},
		Aggregations:      []models.Aggregation{{Field: "age", Operation: "avg", Name: "avg_age"}},
		Exports:           []models.ExportTarget{{Type: "json", Path: "out.json"}},
		ValidationWorkers: 4,
		TransformWorkers:  2,
		BufferSize:        32,
		TimeoutSeconds:    60,
		BatchSize:         100,
		MaxRecords:        1000,
		DLQPath:           "dlq.json",
	}

	cfg := req.ToConfig()
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "source-1" {
		t.Fatalf("unexpected sources: %+v", cfg.Sources)
	}
	if cfg.ValidationWorkers != 4 || cfg.TransformWorkers != 2 || cfg.BufferSize != 32 || cfg.DLQPath != "dlq.json" {
		t.Fatalf("unexpected config mapping: %+v", cfg)
	}

	// Test backward-compatibility alias dead_letter_path
	reqAlias := request.CreatePipelineRequest{
		DeadLetterPath: "fallback-dlq.json",
	}
	cfgAlias := reqAlias.ToConfig()
	if cfgAlias.DLQPath != "fallback-dlq.json" {
		t.Fatalf("expected DLQPath fallback-dlq.json from DeadLetterPath alias, got %s", cfgAlias.DLQPath)
	}
}

func TestCreatePipelineRequestJSON(t *testing.T) {
	raw := `{"sources":[{"type":"csv","name":"s"}],"exports":[{"type":"json"}],"validation_workers":2}`
	var req request.CreatePipelineRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("failed unmarshaling json: %v", err)
	}
	if len(req.Sources) != 1 || req.ValidationWorkers != 2 {
		t.Fatalf("unexpected unmarshaled fields: %+v", req)
	}
}
