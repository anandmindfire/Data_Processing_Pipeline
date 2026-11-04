---
name: "Bug Report"
about: "Submit a bug report to help improve the Data Processing Pipeline"
title: "[BUG]: "
labels: ["bug", "needs-triage"]
assignees: ""
---

## Description
A clear and concise description of what the bug is.

---

## Affected Pipeline Component
Please mark the affected component(s):

- [ ] Ingestion (CSV, JSON, HTTP/REST readers, SSRF validation, retry handling)
- [ ] Validation (Schema rules, bounds checking, string normalization, type coercion)
- [ ] Transformation (Field mapping, normalizers, data enrichment)
- [ ] Aggregation (Group-by aggregators, summary statistics)
- [ ] Export (CSV, JSON, SQLite exports, Dead-Letter queue)
- [ ] Job Manager & Persistence (Repository state, lifecycle states, cancellation)
- [ ] REST API & Routing (chi router, controllers, middleware, request validation)
- [ ] Observability (Prometheus metrics, slog structured logs, healthcheck)
- [ ] Containerization (Dockerfile, Docker Compose, distroless runtime)

---

## Steps to Reproduce
Steps to reproduce the behavior:
1. Submit a pipeline job with the following configuration `...`
2. Send request to endpoint `...`
3. Observe unexpected behavior or error response

---

## Sample Pipeline Configuration Payload
```json
{
  "sources": [
    { "type": "csv", "name": "sample", "path": "path/to/data.csv" }
  ],
  "rules": [],
  "exports": []
}
```

---

## Expected Behavior
A clear and concise description of what you expected to happen.

---

## Actual Behavior & Logs
Include terminal logs, structured JSON logs, or API responses:
```
<paste error logs or stack trace here>
```

---

## Environment Information
- **OS:** (e.g. macOS 14.5, Ubuntu 22.04)
- **Go Version:** (run `go version`)
- **Docker Version (if applicable):** (run `docker version`)
- **Storage Backend:** (In-Memory, File, SQLite)

---

## Additional Context
Add any other context about the problem here (e.g., dataset volume, concurrency load, network environment).

