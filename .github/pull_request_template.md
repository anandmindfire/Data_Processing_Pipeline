## Description

Please provide a concise summary of the changes introduced by this pull request and the motivation behind them.

- **What problem does this PR solve?**
- **What is the solution?**

---

## Type of Change

Please select all that apply:

- [ ] `feat`: New feature or capability (non-breaking change which adds functionality)
- [ ] `fix`: Bug fix (non-breaking change which fixes an issue)
- [ ] `refactor`: Code change that neither fixes a bug nor adds a feature
- [ ] `perf`: Performance improvement (e.g. reduced allocations, worker tuning, caching)
- [ ] `test`: Adding missing tests or improving existing test suites
- [ ] `docs`: Documentation updates (README, design report, OpenAPI spec)
- [ ] `ci`: Changes to CI/CD workflows or tooling configuration
- [ ] `chore`: Maintenance tasks, dependency updates, or build process improvements

---

## Pipeline Components Modified

- [ ] Ingestion (CSV, JSON, HTTP/REST readers, retry mechanism)
- [ ] Validation (Schema rules, bounds checking, regex validation)
- [ ] Transformation (Type casting, normalization, enrichment)
- [ ] Aggregation (Group-by aggregators, summary calculations)
- [ ] Export (CSV, JSON, SQLite / SQL batch export)
- [ ] Job Manager & Storage (Repository implementations, lifecycle states, cancellation)
- [ ] REST API & Routing (chi router, request handling, OpenAPI/Swagger docs)
- [ ] Telemetry & Metrics (Prometheus exposition, slog structured logging)
- [ ] Configuration & Deployment (12-factor env vars, Dockerfile, Docker Compose)

---

## Verification & Testing Checklist

Please confirm that all local verification steps have passed before requesting review:

- [ ] **Formatting:** Code adheres to standard Go formatting (`make fmt` or `gofmt -s -w .`)
- [ ] **Static Analysis:** Passes static checks with zero warnings (`make vet` or `go vet ./...`)
- [ ] **Unit & Integration Tests:** All test suites pass cleanly (`make test` or `go test -v ./...`)
- [ ] **Concurrency & Race Detection:** Concurrency test suite passes with **zero data races** (`make race` or `go test -race -count=1 ./...`)
- [ ] **Build:** Server binary compiles cleanly without warnings (`make build`)
- [ ] **Container Build (if applicable):** Multi-stage Docker image builds cleanly (`make docker-build`)
- [ ] **API Documentation:** Swagger/OpenAPI spec (`docs/openapi.json` & `/docs`) updated if API endpoints or schemas changed

---

## How Was This Tested?

Please describe the manual or automated test scenarios executed:

```bash
# Example test commands executed:
make check
go test -race -count=1 ./...
```

---

## Breaking Changes / Deployment Considerations

- [ ] **Breaking API change:** Existing API consumers will require updates
- [ ] **Configuration change:** Requires new environment variables or config updates
- [ ] **No breaking changes**

