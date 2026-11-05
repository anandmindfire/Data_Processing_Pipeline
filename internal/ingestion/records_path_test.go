package ingestion

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func collect(t *testing.T, doc, path string, max int) ([]models.Record, error) {
	t.Helper()
	var out []models.Record
	err := readJSONAtPath(context.Background(), strings.NewReader(doc), path, max, func(_ int, r models.Record) error {
		out = append(out, r)
		return nil
	})
	return out, err
}

func TestReadJSONAtPath(t *testing.T) {
	recs, err := collect(t, `{"info":{"n":2},"results":[{"name":"a","age":30},{"name":"b"}]}`, "results", 0)
	if err != nil || len(recs) != 2 || recs[0]["age"] != 30.0 {
		t.Fatalf("array path: %v %+v", err, recs)
	}
	recs, err = collect(t, `{"latitude":20,"current_weather":{"temperature":31.4,"windspeed":7}}`, "current_weather", 0)
	if err != nil || len(recs) != 1 || recs[0]["temperature"] != 31.4 {
		t.Fatalf("object path: %v %+v", err, recs)
	}
	recs, err = collect(t, `{"data":{"items":[{"k":1},{"k":2},{"k":3}]}}`, "data.items", 2)
	if err != nil || len(recs) != 2 {
		t.Fatalf("nested path with max_records: %v %+v", err, recs)
	}
	if recs, err = collect(t, `[{"k":1},{"k":2}]`, "1", 0); err != nil || recs[0]["k"] != 2.0 {
		t.Fatalf("array index: %v %+v", err, recs)
	}
	for _, bad := range []struct{ doc, path string }{
		{`{"a":1}`, "missing"},
		{`{"a":1}`, "a"},
		{`{"a":[1,2]}`, "a"},
		{`{"a":[{}]}`, "a.5"},
	} {
		if _, err := collect(t, bad.doc, bad.path, 0); !errors.Is(err, ErrInvalidFormat) {
			t.Fatalf("%s @ %s: expected ErrInvalidFormat, got %v", bad.doc, bad.path, err)
		}
	}
}

func TestLimitedReaderFailsInsteadOfTruncating(t *testing.T) {
	b, err := io.ReadAll(newLimitedReader(strings.NewReader("0123456789"), 10))
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("exactly at limit should succeed: %q %v", b, err)
	}
	b, err = io.ReadAll(newLimitedReader(strings.NewReader("0123456789X"), 10))
	if !errors.Is(err, ErrSourceTooLarge) || len(b) > 10 {
		t.Fatalf("over limit should fail without returning extra bytes: %q %v", b, err)
	}
	if b, _ := io.ReadAll(newLimitedReader(strings.NewReader("abc"), 0)); string(b) != "abc" {
		t.Fatal("limit 0 means unlimited")
	}
}

func TestReaderEnforcesMaxSourceBytes(t *testing.T) {
	r := NewReader(nil, WithMaxSourceBytes(16))
	out := make(chan Item, 100)
	err := r.Read(context.Background(), models.Source{Type: "csv", Name: "big", Path: "../../samples/input.csv"}, 0, out, nil)
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("expected ErrSourceTooLarge, got %v", err)
	}
}
