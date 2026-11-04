package aggregation

import (
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
	deptSum := res["dept_sum"].(map[string]float64)
	if deptSum["Engineering"] != 300.0 || deptSum["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_sum: %v", deptSum)
	}

	// 2. Check dept_avg
	deptAvg := res["dept_avg"].(map[string]float64)
	if deptAvg["Engineering"] != 150.0 || deptAvg["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_avg: %v", deptAvg)
	}

	// 3. Check dept_count
	deptCount := res["dept_count"].(map[string]float64)
	if deptCount["Engineering"] != 2.0 || deptCount["Sales"] != 1.0 {
		t.Fatalf("unexpected dept_count: %v", deptCount)
	}

	// 4. Check dept_min and dept_max
	deptMin := res["dept_min"].(map[string]float64)
	if deptMin["Engineering"] != 100.0 || deptMin["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_min: %v", deptMin)
	}
	deptMax := res["dept_max"].(map[string]float64)
	if deptMax["Engineering"] != 200.0 || deptMax["Sales"] != 150.0 {
		t.Fatalf("unexpected dept_max: %v", deptMax)
	}

	// 5. Check total_sum
	totalSum := res["total_sum"].(map[string]float64)
	if totalSum["_all"] != 450.0 {
		t.Fatalf("unexpected total_sum: %v", totalSum)
	}
}
