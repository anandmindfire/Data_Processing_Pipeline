package validator_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
)

// TestSampleJobSpecsAreValid keeps the shipped sample job specs in sync with request validation,
// using the same defaults as a local run from the repository root (INPUT_DIRS=samples).
func TestSampleJobSpecsAreValid(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	wd, _ := os.Getwd()
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()

	v := validator.NewRequestValidator(validator.DefaultPolicy("exports", "samples"))
	for _, name := range []string{"samples/job.json", "samples/mixed_job.json"} {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var req request.CreatePipelineRequest
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		cfg := req.ToConfig()
		if err := v.Validate(&cfg); err != nil {
			t.Fatalf("%s is rejected by the validator: %v", name, err)
		}
	}
}
