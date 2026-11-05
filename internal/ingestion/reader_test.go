package ingestion

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestSourceReaderCSV(t *testing.T) {
	p := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(p, []byte("id,name\n1,Ada\n2,Linus\n"), 0644); err != nil {
		t.Fatal(err)
	}

	reader := NewReader(nil)
	out := make(chan Item, 4)
	count := 0
	src := models.Source{Type: "csv", Name: "local", Path: p}

	if err := reader.Read(context.Background(), src, 0, out, func() { count++ }); err != nil {
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

func TestSourceReaderRemoteCSV(t *testing.T) {
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			csvBody := "country,cases,deaths\nUSA,100,2\nUK,50,1\n"
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(csvBody)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	reader := NewReader(client)
	out := make(chan Item, 4)
	src := models.Source{
		Type: "csv",
		Name: "remote-covid-csv",
		URL:  "https://example.org/covid.csv",
	}

	if err := reader.Read(context.Background(), src, 0, out, nil); err != nil {
		t.Fatalf("remote CSV read failed: %v", err)
	}
	close(out)

	if len(out) != 2 {
		t.Fatalf("expected 2 items from remote CSV, got %d", len(out))
	}
	item := <-out
	if item.Record["country"] != "USA" || item.Record["cases"] != "100" {
		t.Fatalf("unexpected record: %v", item.Record)
	}
}

func TestSourceReaderHTTP(t *testing.T) {
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("X-Custom") != "token-123" {
				t.Errorf("missing expected header X-Custom, got %q", req.Header.Get("X-Custom"))
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`[{"id":1,"city":"Berlin"}]`)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	reader := NewReader(client)
	out := make(chan Item, 2)
	src := models.Source{
		Type:    "http",
		Name:    "remote",
		URL:     "http://service.test/cities",
		Headers: map[string]string{"X-Custom": "token-123"},
	}

	if err := reader.Read(context.Background(), src, 0, out, nil); err != nil {
		t.Fatal(err)
	}
	close(out)

	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	item := <-out
	if item.Record["city"] != "Berlin" {
		t.Fatalf("unexpected record: %v", item.Record)
	}
}

func TestSourceReaderHTTPRetry(t *testing.T) {
	attempts := 0
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				// Transient failure on first attempt
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       io.NopCloser(strings.NewReader("service temporarily unavailable")),
					Header:     make(http.Header),
				}, nil
			}
			// Success on second attempt
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`[{"retried":true}]`)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	reader := NewReader(client)
	out := make(chan Item, 2)
	src := models.Source{
		Type: "http",
		Name: "retry-api",
		URL:  "https://api.example.com/retry",
	}

	if err := reader.Read(context.Background(), src, 0, out, nil); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	close(out)

	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record after retry, got %d", len(out))
	}
}

func TestSourceReaderTable(t *testing.T) {
	dir := t.TempDir()

	validJSON := filepath.Join(dir, "valid.json")
	_ = os.WriteFile(validJSON, []byte(`[{"id":1},{"id":2},{"id":3}]`), 0644)

	singleJSON := filepath.Join(dir, "single.json")
	_ = os.WriteFile(singleJSON, []byte(`{"id":42}`), 0644)

	tests := []struct {
		name          string
		src           models.Source
		max           int
		expectedCount int
		expectErr     bool
	}{
		{
			name:          "JSON array with max records limit",
			src:           models.Source{Type: "json", Name: "j1", Path: validJSON},
			max:           2,
			expectedCount: 2,
			expectErr:     false,
		},
		{
			name:          "Single JSON object",
			src:           models.Source{Type: "json", Name: "j2", Path: singleJSON},
			max:           0,
			expectedCount: 1,
			expectErr:     false,
		},
		{
			name:          "Missing path and URL error",
			src:           models.Source{Type: "csv", Name: "err1"},
			max:           0,
			expectedCount: 0,
			expectErr:     true,
		},
		{
			name:          "Unsupported source type without path or url",
			src:           models.Source{Type: "unknown", Name: "err2"},
			max:           0,
			expectedCount: 0,
			expectErr:     true,
		},
	}

	reader := NewReader(nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := make(chan Item, 10)
			count := 0
			err := reader.Read(context.Background(), tc.src, tc.max, out, func() { count++ })
			close(out)

			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if !tc.expectErr && count != tc.expectedCount {
				t.Fatalf("expected %d records, got %d", tc.expectedCount, count)
			}
		})
	}
}

func TestCSVToleratesStrayQuotesInHeader(t *testing.T) {
	var got []models.Record
	err := readCSV(context.Background(), strings.NewReader("\"Index\", Height(Inches)\", \"Weight(Pounds)\"\n1, 65.78, 112.99\n"), 0,
		func(_ int, r models.Record) error { got = append(got, r); return nil })
	if err != nil || len(got) != 1 || got[0]["Height(Inches)"] != "65.78" || got[0]["Index"] != "1" || got[0]["Weight(Pounds)"] != "112.99" {
		t.Fatalf("unexpected parse: %v %+v", err, got)
	}
}
