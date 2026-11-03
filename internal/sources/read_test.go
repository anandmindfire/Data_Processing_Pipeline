package sources

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anandkr47/data-processing-pipeline/internal/domain"
)

func TestReadCSV(t *testing.T) {
	p := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(p, []byte("id,name\n1,Ada\n2,Linus\n"), 0644); err != nil {
		t.Fatal(err)
	}
	out := make(chan Item, 4)
	count := 0
	if err := Read(context.Background(), domain.Source{Type: "csv", Name: "local", Path: p}, http.DefaultClient, 0, out, func() { count++ }); err != nil {
		t.Fatal(err)
	}
	close(out)
	if count != 2 {
		t.Fatalf("got %d records", count)
	}
	first := <-out
	if first.Record["name"] != "Ada" {
		t.Fatalf("record=%v", first.Record)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestReadHTTPJSON(t *testing.T) {
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("X-Custom") != "test-token" {
				t.Errorf("missing expected header X-Custom, got %q", req.Header.Get("X-Custom"))
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`[{"id":1,"name":"Alpha"},{"id":2,"name":"Beta"}]`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	out := make(chan Item, 4)
	src := domain.Source{
		Type:    "http",
		Name:    "remote",
		URL:     "http://api.local/data",
		Headers: map[string]string{"X-Custom": "test-token"},
	}
	if err := Read(context.Background(), src, client, 0, out, func() {}); err != nil {
		t.Fatal(err)
	}
	close(out)
	if len(out) != 2 {
		t.Fatalf("got %d records", len(out))
	}
	item1 := <-out
	if item1.Record["name"] != "Alpha" {
		t.Fatalf("unexpected record: %v", item1.Record)
	}
}

func TestReadJSONArrayAndMaxRecords(t *testing.T) {
	p := filepath.Join(t.TempDir(), "data.json")
	content := `[{"id": 1, "city": "Seattle"}, {"id": 2, "city": "Austin"}, {"id": 3, "city": "Boston"}]`
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Test with max = 2
	out := make(chan Item, 4)
	discovered := 0
	src := domain.Source{Type: "json", Name: "cities", Path: p}
	if err := Read(context.Background(), src, http.DefaultClient, 2, out, func() { discovered++ }); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	close(out)
	if discovered != 2 {
		t.Fatalf("expected 2 discovered records due to max=2, got %d", discovered)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 records in out channel, got %d", len(out))
	}
}

func TestReadJSONSingleObject(t *testing.T) {
	p := filepath.Join(t.TempDir(), "single.json")
	content := `{"id": 100, "metric": "cpu", "value": 98.6}`
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	out := make(chan Item, 2)
	discovered := 0
	src := domain.Source{Type: "json", Name: "metric", Path: p}
	if err := Read(context.Background(), src, http.DefaultClient, 0, out, func() { discovered++ }); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	close(out)
	if discovered != 1 {
		t.Fatalf("expected 1 discovered record, got %d", discovered)
	}
	item := <-out
	if item.Record["metric"] != "cpu" {
		t.Fatalf("unexpected metric: %v", item.Record)
	}
}

func TestReadErrors(t *testing.T) {
	out := make(chan Item, 2)

	// Missing path for CSV
	err := Read(context.Background(), domain.Source{Type: "csv", Name: "missing"}, http.DefaultClient, 0, out, func() {})
	if err == nil {
		t.Fatal("expected error for empty path")
	}

	// Missing URL for HTTP
	err = Read(context.Background(), domain.Source{Type: "http", Name: "missing-url"}, http.DefaultClient, 0, out, func() {})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}

	// Unsupported type
	err = Read(context.Background(), domain.Source{Type: "kafka", Name: "unsupported"}, http.DefaultClient, 0, out, func() {})
	if err == nil {
		t.Fatal("expected error for unsupported source type")
	}

	// HTTP 500 error
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("internal server error")),
			}, nil
		}),
	}
	err = Read(context.Background(), domain.Source{Type: "http", Name: "fail", URL: "http://example.com"}, client, 0, out, func() {})
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}
