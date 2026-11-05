#!/usr/bin/env bash
# Container smoke test: start the image, wait for readiness, run the sample job end to end, and
# verify progress, results, exports and the security headers. Used by CI and `make docker-smoke`.
#
# Usage: scripts/smoke_test.sh <image[:tag]> [host-port]
set -euo pipefail

IMAGE="${1:?usage: smoke_test.sh <image> [port]}"
PORT="${2:-18080}"
NAME="pipeline-smoke-$$"
API_KEY="smoke-test-key-0123456789"
BASE="http://127.0.0.1:${PORT}"

cleanup() { docker logs "$NAME" >/tmp/"$NAME".log 2>&1 || true; docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $*" >&2; echo "--- container logs ---" >&2; docker logs "$NAME" >&2 || true; exit 1; }

docker run -d --name "$NAME" -p "127.0.0.1:${PORT}:8080" \
  -e API_KEY="$API_KEY" --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges:true \
  "$IMAGE" >/dev/null

echo "waiting for readiness..."
for _ in $(seq 1 30); do
  if curl -fsS "$BASE/readyz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "$BASE/readyz" >/dev/null || fail "service never became ready"

status=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/v1/pipelines")
[ "$status" = "401" ] || fail "expected 401 without API key, got $status"

headers=$(curl -sS -D - -o /dev/null -H "X-API-Key: $API_KEY" "$BASE/api/v1/pipelines")
grep -qi '^content-security-policy:' <<<"$headers" || fail "missing Content-Security-Policy header"
grep -qi '^x-content-type-options: nosniff' <<<"$headers" || fail "missing X-Content-Type-Options header"

job=$(curl -fsS -X POST "$BASE/api/v1/pipelines" -H "X-API-Key: $API_KEY" \
  -H 'Content-Type: application/json' --data @samples/job.json) || fail "job submission failed"
id=$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' <<<"$job")
[ -n "$id" ] || fail "no job id in response: $job"
echo "submitted job $id"

for _ in $(seq 1 30); do
  progress=$(curl -fsS -H "X-API-Key: $API_KEY" "$BASE/api/v1/pipelines/$id/progress")
  grep -q '"status":"COMPLETED"' <<<"$progress" && break
  grep -q '"status":"\(FAILED\|CANCELLED\)"' <<<"$progress" && fail "job did not complete: $progress"
  sleep 1
done
grep -q '"status":"COMPLETED"' <<<"$progress" || fail "job timed out: $progress"
grep -q '"processed":6' <<<"$progress" || fail "unexpected processed count: $progress"

results=$(curl -fsS -H "X-API-Key: $API_KEY" "$BASE/api/v1/pipelines/$id/results")
grep -q '"salary_total"' <<<"$results" || fail "aggregations missing from results"
grep -q '/app/exports/employees.json' <<<"$results" || fail "export path missing from results"

errors=$(curl -fsS -H "X-API-Key: $API_KEY" "$BASE/api/v1/pipelines/$id/errors")
grep -q '"total":1' <<<"$errors" || fail "expected exactly one quarantined record: $errors"

curl -fsS -H "X-API-Key: $API_KEY" "$BASE/metrics" | grep -q '^pipeline_jobs_completed_total 1' || fail "metrics not updated"

docker exec "$NAME" /pipeline-server --version >/dev/null || fail "binary not runnable in container"

echo "SMOKE OK: job $id completed, results/errors/metrics verified"
