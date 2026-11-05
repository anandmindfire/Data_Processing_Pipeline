package aggregation

import (
	"math"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

func TestMetricsAggregator(t *testing.T) {
	definitions := []models.Aggregation{
		{GroupBy: "dept", Field: "salary", Operation: "sum", Name: "dept_sum"},
		{GroupBy: "dept", Field: "salary", Operation: "avg", Name: "dept_avg"},
		{GroupBy: "dept", Field: "salary", Operation: "count", Name: "dept_count"},
		{GroupBy: "dept", Field: "salary", Operation: "min", Name: "dept_min"},
		{GroupBy: "dept", Field: "salary", Operation: "max", Name: "dept_max"},
		{Field: "salary", Operation: "sum", Name: "total_sum"},
	}

	aggregator := NewAggregator(definitions)

	records := []models.Record{
		{"dept": "Engineering", "salary": 100.0},
		{"dept": "Engineering", "salary": 200.0},
		{"dept": "Sales", "salary": 150.0},
	}

	for _, r := range records {
		aggregator.Process(r)
	}

	res := aggregator.Finalize()

	// 1. Check dept_sum
	deptSum := assertGroupMap(t, res, "dept_sum")
	if deptSum["Engineering"] != 300.0 || deptSum["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_sum: %v", deptSum)
	}

	// 2. Check dept_avg
	deptAvg := assertGroupMap(t, res, "dept_avg")
	if deptAvg["Engineering"] != 150.0 || deptAvg["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_avg: %v", deptAvg)
	}

	// 3. Check dept_count
	deptCount := assertGroupMap(t, res, "dept_count")
	if deptCount["Engineering"] != 2.0 || deptCount["Sales"] != 1.0 {
		t.Fatalf("unexpected dept_count: %v", deptCount)
	}

	// 4. Check dept_min and dept_max
	deptMin := assertGroupMap(t, res, "dept_min")
	if deptMin["Engineering"] != 100.0 || deptMin["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_min: %v", deptMin)
	}
	deptMax := assertGroupMap(t, res, "dept_max")
	if deptMax["Engineering"] != 200.0 || deptMax["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_max: %v", deptMax)
	}

	// 5. Check total_sum
	totalSum := assertGroupMap(t, res, "total_sum")
	if totalSum["_all"] != 450.0 {
		t.Fatalf("unexpected total_sum: %v", totalSum)
	}
}

func assertGroupMap(t *testing.T, res map[string]any, key string) map[string]float64 {
	t.Helper()
	m, ok := res[key].(map[string]float64)
	if !ok {
		t.Fatalf("expected map[string]float64 for key %q, got %T", key, res[key])
	}
	return m
}

func TestMergeMatchesSequentialAggregation(t *testing.T) {
	defs := []models.Aggregation{
		{GroupBy: "g", Field: "v", Operation: "count", Name: "c"},
		{GroupBy: "g", Field: "v", Operation: "sum", Name: "s"},
		{GroupBy: "g", Field: "v", Operation: "avg", Name: "a"},
		{GroupBy: "g", Field: "v", Operation: "min", Name: "lo"},
		{GroupBy: "g", Field: "v", Operation: "max", Name: "hi"},
	}
	var records []models.Record
	for i := 0; i < 1000; i++ {
		records = append(records, models.Record{"g": []string{"x", "y", "z"}[i%3], "v": float64((i * 37) % 101)})
	}

	seq := NewAggregator(defs)
	for _, r := range records {
		seq.Process(r)
	}

	final := NewAggregator(defs)
	for w := 0; w < 4; w++ {
		part := NewAggregator(defs)
		for i := w; i < len(records); i += 4 {
			part.Process(records[i])
		}
		final.Merge(part)
	}

	want, got := seq.Finalize(), final.Finalize()
	for name, groups := range want {
		for g, v := range groups.(map[string]float64) {
			if gv := got[name].(map[string]float64)[g]; math.Abs(gv-v) > 1e-9 {
				t.Fatalf("%s[%s]: merged %v != sequential %v", name, g, gv, v)
			}
		}
	}
}

func TestCountSkipsRecordsWithoutTheField(t *testing.T) {
	agg := NewAggregator([]models.Aggregation{{GroupBy: "gender", Field: "gender", Operation: "count", Name: "by_gender"}})
	for _, r := range []models.Record{{"gender": "f"}, {"gender": "m"}, {"gender": "f"}, {"title": "a post"}, {"gender": ""}} {
		agg.Process(r)
	}
	got := agg.Finalize()["by_gender"].(map[string]float64)
	if len(got) != 2 || got["f"] != 2 || got["m"] != 1 {
		t.Fatalf("expected only records with a gender to be counted, got %v", got)
	}
}
