package validator

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func sandbox(t *testing.T) (inputs, exports, outside string) {
	t.Helper()
	root := t.TempDir()
	inputs, exports, outside = filepath.Join(root, "in"), filepath.Join(root, "out"), filepath.Join(root, "secret")
	for _, d := range []string{inputs, exports, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return inputs, exports, outside
}

func TestResolveInputSandbox(t *testing.T) {
	inputs, _, outside := sandbox(t)
	ok := filepath.Join(inputs, "a.csv")
	_ = os.WriteFile(ok, []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(outside, "s.csv"), []byte("x"), 0o644)
	if err := os.Symlink(outside, filepath.Join(inputs, "link")); err != nil {
		t.Fatal(err)
	}

	if got, err := resolveInput(ok, []string{inputs}); err != nil || !strings.HasSuffix(got, "a.csv") {
		t.Fatalf("expected allowed path, got %q %v", got, err)
	}
	for _, bad := range []string{
		filepath.Join(outside, "s.csv"),
		filepath.Join(inputs, "..", "secret", "s.csv"),
		filepath.Join(inputs, "link", "s.csv"), // symlink escape
		inputs,                                 // the directory itself
	} {
		if _, err := resolveInput(bad, []string{inputs}); !errors.Is(err, ErrPathNotAllowed) {
			t.Fatalf("%s: expected ErrPathNotAllowed, got %v", bad, err)
		}
	}
	if _, err := resolveInput(ok, nil); !errors.Is(err, ErrPathNotAllowed) {
		t.Fatalf("expected file sources disabled without INPUT_DIRS, got %v", err)
	}
}

func TestResolveOutputSandbox(t *testing.T) {
	_, exports, outside := sandbox(t)
	if err := os.Symlink(outside, filepath.Join(exports, "link")); err != nil {
		t.Fatal(err)
	}
	for _, good := range []string{"out.json", "nested/dir/out.csv", filepath.Join(exports, "x.db")} {
		got, err := resolveOutput(good, exports)
		if err != nil || !strings.Contains(got, string(os.PathSeparator)+"out"+string(os.PathSeparator)) {
			t.Fatalf("%s: expected path in export dir, got %q %v", good, got, err)
		}
	}
	for _, bad := range []string{"../escape.json", filepath.Join(outside, "x.json"), "link/x.json", "/etc/passwd"} {
		if _, err := resolveOutput(bad, exports); !errors.Is(err, ErrPathNotAllowed) {
			t.Fatalf("%s: expected ErrPathNotAllowed, got %v", bad, err)
		}
	}
}

func TestValidateCanonicalizesAndReportsAllErrors(t *testing.T) {
	inputs, exports, _ := sandbox(t)
	in := filepath.Join(inputs, "a.csv")
	_ = os.WriteFile(in, []byte("x"), 0o644)
	v := NewRequestValidator(DefaultPolicy(exports, inputs))

	cfg := &models.Config{
		Sources: []models.Source{{Type: "CSV", Name: "a", Path: in}},
		Exports: []models.ExportTarget{{Type: "JSON", Path: "r.json"}},
		DLQPath: "dlq.json",
	}
	if err := v.Validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Sources[0].Type != "csv" || cfg.Exports[0].Type != "json" ||
		!filepath.IsAbs(cfg.Exports[0].Path) || !filepath.IsAbs(cfg.DLQPath) {
		t.Fatalf("expected canonicalized spec, got %+v", cfg)
	}

	bad := &models.Config{
		Sources:           []models.Source{{Type: "csv", Name: "a", Path: in}, {Type: "csv", Name: "a", Path: in}},
		Exports:           []models.ExportTarget{{Type: "json"}},
		ValidationWorkers: 1000,
	}
	err := v.Validate(bad)
	if err == nil || !strings.Contains(err.Error(), "duplicate source name") || !strings.Contains(err.Error(), "validation_workers") {
		t.Fatalf("expected all errors reported together, got %v", err)
	}
}

func TestValidateRulesAndAggregations(t *testing.T) {
	_, exports, _ := sandbox(t)
	v := NewRequestValidator(DefaultPolicy(exports))

	minVal := 10.0
	maxVal := 5.0
	badRules := &models.Config{
		Sources: []models.Source{{Type: "http", Name: "s1", URL: "https://example.com/data.json"}},
		Exports: []models.ExportTarget{{Type: "json", Path: "out.json"}},
		Rules: []models.Rule{
			{Field: "", Type: "invalid_type", Normalize: "invalid_norm", Min: &minVal, Max: &maxVal, Pattern: "[a-"},
		},
		Aggregations: []models.Aggregation{
			{Field: "", Operation: "invalid_op"},
		},
	}
	err := v.Validate(badRules)
	if err == nil {
		t.Fatalf("expected validation errors for bad rules and aggs")
	}
	errStr := err.Error()
	for _, expected := range []string{
		"field is required",
		"type must be string, number or boolean",
		"normalize must be trim, lower, upper or title",
		"min cannot be greater than max",
		"invalid pattern",
		"operation must be one of count, sum, avg, min, max",
	} {
		if !strings.Contains(errStr, expected) {
			t.Errorf("missing expected error substring %q in: %s", expected, errStr)
		}
	}
}
