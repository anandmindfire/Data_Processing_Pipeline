#!/usr/bin/env bash
# Trigger a sample pipeline execution against the running local server
set -euo pipefail

PORT="${PORT:-8080}"
SERVER_URL="${API_BASE_URL:-${SERVER_URL:-http://localhost:${PORT}}}"
API_PATH="${API_BASE_PATH:-/api/v1}"

echo "Submitting sample pipeline job to ${SERVER_URL}${API_PATH}..."

curl -s -X POST "${SERVER_URL}${API_PATH}/pipelines" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {
        "type": "csv",
        "name": "sample_csv",
        "path": "samples/input.csv"
      }
    ],
    "rules": [
      {
        "field": "score",
        "required": true,
        "type": "number",
        "min": 0,
        "max": 100
      }
    ],
    "exports": [
      {
        "type": "json",
        "path": "exports/sample_output.json"
      }
    ]
  }' | jq .

