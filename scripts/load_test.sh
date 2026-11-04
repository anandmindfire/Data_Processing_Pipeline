#!/usr/bin/env bash
# ==============================================================================
# Pipeline Server Load & Concurrency Benchmark Script
# ==============================================================================
set -euo pipefail

PORT="${PORT:-8080}"
BASE_URL="${API_BASE_URL:-${BASE_URL:-http://localhost:${PORT}}}"
API_PATH="${API_BASE_PATH:-/api/v1}"
CONCURRENCY="${CONCURRENCY:-5}"
ITERATIONS="${ITERATIONS:-20}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/pipeline_load_test}"

echo "======================================================================"
echo " Starting Pipeline Load Test"
echo " Target URL:   $BASE_URL"
echo " Concurrency:  $CONCURRENCY concurrent workers"
echo " Total Jobs:   $ITERATIONS"
echo "======================================================================"

# 1. Health check
echo "[1/4] Checking server health..."
HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/healthz" || echo "failed")
if [ "$HEALTH_STATUS" != "200" ]; then
    echo "ERROR: Server at $BASE_URL is not healthy (HTTP $HEALTH_STATUS). Start the server first."
    exit 1
fi
echo "Server is healthy."

# 2. Setup temporary data directory
mkdir -p "$OUTPUT_DIR"
SAMPLE_CSV="$OUTPUT_DIR/bench.csv"
cat <<EOF > "$SAMPLE_CSV"
id,name,department,salary
1,Alice,Engineering,125000
2,Bob,Sales,90000
3,Charlie,Engineering,140000
4,Diana,Marketing,85000
5,Evan,Engineering,110000
EOF

JOB_PAYLOAD=$(cat <<EOF
{
  "sources": [
    {"type": "csv", "name": "bench-src", "path": "$SAMPLE_CSV"}
  ],
  "rules": [
    {"field": "name", "type": "string", "required": true},
    {"field": "salary", "type": "number", "required": true, "min": 0}
  ],
  "aggregations": [
    {"group_by": "department", "field": "salary", "operation": "avg", "name": "dept_avg"},
    {"group_by": "department", "field": "salary", "operation": "count", "name": "dept_count"}
  ],
  "exports": [
    {"type": "json", "path": "$OUTPUT_DIR/out_\$ID.json"}
  ],
  "validation_workers": 2,
  "transform_workers": 2
}
EOF
)

# 3. Concurrent Execution
echo "[2/4] Disconnecting $ITERATIONS jobs across $CONCURRENCY parallel workers..."
START_TIME=$(date +%s)

SUBMIT_JOB() {
    local index=$1
    local payload="${JOB_PAYLOAD//\$ID/$index}"
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL$API_PATH/pipelines" \
        -H "Content-Type: application/json" \
        -d "$payload")
    if [ "$code" = "202" ]; then
        echo "OK"
    else
        echo "ERR:$code"
    fi
}
export -f SUBMIT_JOB
export BASE_URL API_PATH JOB_PAYLOAD

RESULTS_FILE="$OUTPUT_DIR/results.txt"
rm -f "$RESULTS_FILE"

# Run parallel worker loops
for ((i=1; i<=ITERATIONS; i++)); do
    SUBMIT_JOB "$i" >> "$RESULTS_FILE" &
    if (( i % CONCURRENCY == 0 )); then
        wait
    fi
done
wait

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
if [ "$DURATION" -eq 0 ]; then DURATION=1; fi

# 4. Results calculation
SUCCESS_COUNT=$(grep -c "^OK" "$RESULTS_FILE" || true)
FAIL_COUNT=$(grep -v "^OK" "$RESULTS_FILE" | wc -l | tr -d ' ' || true)
RPS=$((ITERATIONS / DURATION))

echo "======================================================================"
echo " Benchmark Complete"
echo " Duration:     ${DURATION}s"
echo " Submitted:    $ITERATIONS jobs"
echo " Accepted:     $SUCCESS_COUNT"
echo " Rejected:     $FAIL_COUNT"
echo " Throughput:   ~$RPS jobs/second"
echo "======================================================================"

echo "[3/4] Fetching pipeline metrics summary..."
curl -s "$BASE_URL/metrics" | grep -E '^data_pipeline_' || true

echo "[4/4] Cleaning up benchmark test files..."
rm -rf "$OUTPUT_DIR"
echo "Load test finished successfully."

