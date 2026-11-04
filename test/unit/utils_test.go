package unit_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/pkg/response"
	"github.com/anandmindfire/data-processing-pipeline/pkg/utils"
)

func TestUtilsFileAndDir(t *testing.T) {
	temp := t.TempDir()
	sub := filepath.Join(temp, "sub", "deep")

	if err := utils.EnsureDir(sub); err != nil {
		t.Fatalf("unexpected EnsureDir error: %v", err)
	}
	if !utils.DirExists(sub) {
		t.Fatal("expected dir to exist")
	}

	filePath := filepath.Join(sub, "test.txt")
	_ = os.WriteFile(filePath, []byte("hello"), 0o600)

	if !utils.FileExists(filePath) {
		t.Fatal("expected file to exist")
	}
}

func TestUtilsPointers(t *testing.T) {
	str := "test"
	p := utils.ToPtr(str)
	if *p != "test" {
		t.Fatalf("expected test, got %s", *p)
	}
	if utils.ValueOrDefault((*string)(nil), "fallback") != "fallback" {
		t.Fatal("expected fallback")
	}
}

func TestUtilsJSONAndCSV(t *testing.T) {
	type sample struct {
		Name string `json:"name"`
	}
	s := sample{Name: "pipeline"}
	b, err := utils.ToJSON(s)
	if err != nil {
		t.Fatalf("failed ToJSON: %v", err)
	}

	var parsed sample
	if err := utils.FromJSON(b, &parsed); err != nil || parsed.Name != "pipeline" {
		t.Fatalf("failed FromJSON: %v", err)
	}

	temp := t.TempDir()
	csvPath := filepath.Join(temp, "test.csv")
	rows := [][]string{
		{"a", "b"},
		{"1", "2"},
	}
	if err := utils.WriteCSVFile(csvPath, rows); err != nil {
		t.Fatalf("WriteCSVFile error: %v", err)
	}
	readRows, err := utils.ReadCSVFile(csvPath)
	if err != nil || len(readRows) != 2 {
		t.Fatalf("ReadCSVFile error: %v", err)
	}
}

func TestResponseHelpers(t *testing.T) {
	rec := httptest.NewRecorder()
	response.JSON(rec, http.StatusOK, map[string]string{"status": "ok"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	recErr := httptest.NewRecorder()
	response.Error(recErr, http.StatusBadRequest, "bad request")
	if recErr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recErr.Code)
	}
}

func TestTimeUtils(t *testing.T) {
	now := utils.NowUTC()
	iso := utils.FormatISO8601(now)
	if iso == "" {
		t.Fatal("expected non-empty ISO8601 string")
	}
	elapsed := utils.ElapsedMilliseconds(now, now.Add(100*time.Millisecond))
	if elapsed < 90 || elapsed > 150 {
		t.Fatalf("unexpected elapsed ms: %v", elapsed)
	}
}
