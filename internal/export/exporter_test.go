package export

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

var testAggregates = map[string]any{"sum:amount": map[string]float64{"_all": 3}}

func exportAll(t *testing.T, targets []models.ExportTarget, batches ...[]models.Record) Session {
	t.Helper()
	s, err := NewExporter(t.TempDir()).Open("job-1", targets)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, b := range batches {
		if err := s.Write(b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := s.Close(testAggregates); err != nil {
		t.Fatalf("close: %v", err)
	}
	return s
}

func TestJSONSinkStreamsArrayAndSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	s := exportAll(t, []models.ExportTarget{{Type: "json", Path: path}},
		[]models.Record{{"id": 1, "name": "Alpha"}}, []models.Record{{"id": 2, "name": "Beta"}})

	var got []map[string]any
	b, _ := os.ReadFile(path)
	if err := json.Unmarshal(b, &got); err != nil || len(got) != 2 || got[1]["name"] != "Beta" {
		t.Fatalf("unexpected json export (%v): %s", err, b)
	}
	if _, err := os.Stat(path + ".part"); !os.IsNotExist(err) {
		t.Fatal("temporary .part file should be gone after Close")
	}
	sidecar := filepath.Join(dir, "out.aggregations.json")
	if _, err := os.Stat(sidecar); err != nil {
		t.Fatalf("expected aggregations sidecar: %v", err)
	}
	if arts := s.Artifacts(); len(arts) != 2 || s.Primary() != path {
		t.Fatalf("unexpected artifacts %v / primary %s", arts, s.Primary())
	}
}

func TestJSONSinkEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	exportAll(t, []models.ExportTarget{{Type: "json", Path: path}})
	b, _ := os.ReadFile(path)
	if strings.TrimSpace(string(b)) != "[]" {
		t.Fatalf("expected empty array, got %q", b)
	}
}

func TestCSVSinkUnionHeaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	exportAll(t, []models.ExportTarget{{Type: "csv", Path: path}},
		[]models.Record{{"b": "x", "a": 1.5}}, []models.Record{{"c": map[string]any{"k": "v"}}})

	f, _ := os.Open(path)
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows[0], ",") != "a,b,c" || rows[1][0] != "1.5" || rows[2][2] != `{"k":"v"}` {
		t.Fatalf("unexpected csv rows: %v", rows)
	}
}

func TestSQLiteSinkWritesRealDatabase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.db")
	exportAll(t, []models.ExportTarget{{Type: "sqlite", Path: path}},
		[]models.Record{{"id": 1.0, "name": "O'Brien", "ok": true}},
		[]models.Record{{"id": 2.0, "extra": "new column"}})

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	var name string
	if err := db.QueryRow(`SELECT COUNT(*), MAX(name) FROM records`).Scan(&n, &name); err != nil || n != 2 || name != "O'Brien" {
		t.Fatalf("records table: n=%d name=%q err=%v", n, name, err)
	}
	var extra string
	if err := db.QueryRow(`SELECT extra FROM records WHERE id = 2`).Scan(&extra); err != nil || extra != "new column" {
		t.Fatalf("dynamic column: %q %v", extra, err)
	}
	var result string
	if err := db.QueryRow(`SELECT result FROM aggregations WHERE name = 'sum:amount'`).Scan(&result); err != nil || !strings.Contains(result, `"_all":3`) {
		t.Fatalf("aggregations table: %q %v", result, err)
	}
}

func TestSQLDumpEscapesValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.sql")
	exportAll(t, []models.ExportTarget{{Type: "sql", Path: path}}, []models.Record{{`we"ird`: "it's", "n": 2.0}})
	b, _ := os.ReadFile(path)
	content := string(b)
	for _, want := range []string{`"we""ird" TEXT`, `"n" NUMERIC`, `'it''s'`, "INSERT INTO aggregations", "COMMIT;"} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in SQL dump:\n%s", want, content)
		}
	}
}

func TestUnsupportedTargetAbortsOpenedSinks(t *testing.T) {
	dir := t.TempDir()
	_, err := NewExporter(dir).Open("job-1", []models.ExportTarget{
		{Type: "json", Path: filepath.Join(dir, "a.json")},
		{Type: "parquet", Path: filepath.Join(dir, "a.parquet")},
	})
	if err == nil {
		t.Fatal("expected unsupported target error")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("expected no leftover files, got %v", entries)
	}
}

func TestAbortRemovesPartialOutput(t *testing.T) {
	dir := t.TempDir()
	s, err := NewExporter(dir).Open("job-1", []models.ExportTarget{{Type: "csv"}, {Type: "sqlite"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]models.Record{{"a": 1}})
	s.Abort()
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("expected abort to clean up, got %v", entries)
	}
}
