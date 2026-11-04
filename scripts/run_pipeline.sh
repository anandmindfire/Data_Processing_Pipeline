#!/usr/bin/env bash
# Trigger a sample pipeline execution against the running local server
set -euo pipefail

SERVER_URL="${SERVER_URL:-http://localhost:8080}"

echo "Submitting sample pipeline job to ${SERVER_URL}..."

curl -s -X POST "${SERVER_URL}/api/v1/pipelines" \
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

