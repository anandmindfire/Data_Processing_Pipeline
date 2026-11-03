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
			name:          "Missing path error",
			src:           models.Source{Type: "csv", Name: "err1"},
			max:           0,
			expectedCount: 0,
			expectErr:     true,
		},
		{
			name:          "Missing URL error",
			src:           models.Source{Type: "http", Name: "err2"},
			max:           0,
			expectedCount: 0,
			expectErr:     true,
		},
		{
			name:          "Unsupported source type",
			src:           models.Source{Type: "redis", Name: "err3"},
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

