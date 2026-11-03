package export

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/anandkr47/data-processing-pipeline/internal/models"
)

func TestFileExporterTable(t *testing.T) {
	dir := t.TempDir()
	exporter := NewExporter(dir)

	records := []models.Record{
		{"id": 1, "name": "Alpha"},
		{"id": 2, "name": "Beta"},
	}

	tests := []struct {
		name       string
		targets    []models.ExportTarget
		expectErr  bool
		checkFiles []string
	}{
		{
			name: "Export to JSON and CSV",
			targets: []models.ExportTarget{
				{Type: "json", Path: filepath.Join(dir, "out.json")},
				{Type: "csv", Path: filepath.Join(dir, "out.csv")},
			},
			expectErr:  false,
			checkFiles: []string{filepath.Join(dir, "out.json"), filepath.Join(dir, "out.csv")},
		},
		{
			name: "Unsupported target type error",
			targets: []models.ExportTarget{
				{Type: "parquet", Path: filepath.Join(dir, "out.parquet")},
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			primary, err := exporter.Export(records, tc.targets, "job-1")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if !tc.expectErr {
				if primary != tc.targets[0].Path {
					t.Fatalf("expected primary path %s, got: %s", tc.targets[0].Path, primary)
				}
				for _, path := range tc.checkFiles {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("expected exported file %s to exist: %v", path, err)
					}
				}
			}
		})
	}
}

