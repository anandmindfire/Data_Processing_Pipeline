package export

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
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
			name: "Export to JSON, CSV and SQLite",
			targets: []models.ExportTarget{
				{Type: "json", Path: filepath.Join(dir, "out.json")},
				{Type: "csv", Path: filepath.Join(dir, "out.csv")},
				{Type: "sqlite", Path: filepath.Join(dir, "out.sql")},
			},
			expectErr:  false,
			checkFiles: []string{filepath.Join(dir, "out.json"), filepath.Join(dir, "out.csv"), filepath.Join(dir, "out.sql")},
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

				// Check SQLite export content
				sqlPath := filepath.Join(dir, "out.sql")
				sqlBytes, err := os.ReadFile(sqlPath)
				if err != nil {
					t.Fatalf("read sql file: %v", err)
				}
				content := string(sqlBytes)
				if !strings.Contains(content, "CREATE TABLE IF NOT EXISTS records") {
					t.Fatalf("expected CREATE TABLE in SQLite export, got: %s", content)
				}
				if !strings.Contains(content, "INSERT INTO records") {
					t.Fatalf("expected INSERT INTO in SQLite export, got: %s", content)
				}
			}
		})
	}
}
