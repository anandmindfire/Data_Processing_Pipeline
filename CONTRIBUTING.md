# Contributing to Data Processing Pipeline

Thank you for your interest in contributing to the **Data Processing Pipeline**! This document provides guidelines and workflows to ensure smooth, high-quality collaboration across our open-source and development efforts.

---

## Table of Contents
1. [Code of Conduct](#code-of-conduct)
2. [Architecture & Repository Structure](#architecture--repository-structure)
3. [Prerequisites & Local Setup](#prerequisites--local-setup)
4. [Branching & Development Workflow](#branching--development-workflow)
5. [Engineering & Coding Standards](#engineering--coding-standards)
6. [Testing & Race Detection Standards](#testing--race-detection-standards)
7. [Submitting a Pull Request](#submitting-a-pull-request)
8. [Reporting Bugs & Suggesting Enhancements](#reporting-bugs--suggesting-enhancements)

---

## Code of Conduct

We are committed to providing a welcoming, inclusive, and harassment-free environment for all contributors. Please review our full [Code of Conduct](CODE_OF_CONDUCT.md) before participating in our discussions, issues, or pull requests. Please communicate with respect, empathy, and professional integrity at all times.

---

## Architecture & Repository Structure

The project is structured as a **modular monolith** adhering to Clean Architecture principles:

```text
├── api/                            # OpenAPI spec (public contract), embedded into the server
├── cmd/server/                     # Entry point (flags, signals, --healthcheck, --version)
├── internal/                       # All application code; not importable by other modules
│   ├── bootstrap/                  # Composition root: wires config, store, pipeline, services, router
│   ├── config/                     # Defaults, JSON file, .env and environment overrides, validation
│   ├── buildinfo/                  # Version metadata injected with -ldflags
│   ├── api/                        # HTTP layer: handlers, middleware, routes, validator, dto, respond
│   ├── services/                   # Use cases: job lifecycle, progress, results, retention
│   ├── pipeline/                   # Engine: channels, worker pools, aggregation, export stage
│   ├── ingestion/                  # CSV/JSON/HTTP readers, SSRF-safe client, size limits
│   ├── validation/ transformation/ aggregation/ export/
│   ├── storage/                    # Repository contract; SQLite and in-memory implementations
│   ├── metrics/                    # Prometheus collector
│   └── models/                     # Domain types (JobSpec, Job, Record, Metrics, RecordError)
├── test/integration/               # Cross-package end-to-end tests
├── samples/                        # Sample inputs, job specs, recorded responses
├── scripts/                        # smoke_test.sh, load_test.sh, run_pipeline.sh
└── .github/                        # CI, Dependabot, issue and PR templates
```

Conventions: unit tests live next to the code (`foo_test.go`); new packages go under `internal/`
(there is deliberately no `pkg/`, since nothing here is meant to be imported by other modules);
dependencies point inward (`handlers → services → pipeline → stages → models`), and only
`internal/bootstrap` constructs concrete implementations.

---

## Prerequisites & Local Setup

### Prerequisites
- **Go:** `1.26` (the patched toolchain pinned in `go.mod` is downloaded automatically)
- **Tools:** `make tools` installs golangci-lint v2 and govulncheck
- **Make:** GNU Make
- **Docker:** Docker 24+ & Docker Compose v2 (optional for containerized deployments)

### Step-by-Step Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/anandmindfire/Data_Processing_Pipeline.git
   cd Data_Processing_Pipeline
   ```

2. **Initialize local environment configuration:**
   ```bash
   cp .env.example .env
   ```

3. **Verify toolchain and run verification checks:**
   ```bash
   make check
   ```

4. **Start the local pipeline server:**
   ```bash
   make run
   # Server listens on http://localhost:8080
   # Swagger documentation is available at http://localhost:8080/docs
   ```

---

## Branching & Development Workflow

We follow standard GitHub Flow:
- **`main` / `master`**: Production-ready, deployable codebase.
- **`Dev`**: Active development and integration branch.
- Feature branches should be branched from `Dev`:
  - `feature/<short-description>` (e.g., `feature/s3-exporter`)
  - `fix/<issue-number>-<short-description>` (e.g., `fix/ssrf-dns-resolution`)
  - `perf/<short-description>` (e.g., `perf/worker-batching`)

### Commit Convention
We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:
- `feat: add S3 export destination`
- `fix: resolve potential nil map panic in progress tracker`
- `test: add concurrent stress tests for worker pool`
- `docs: update OpenAPI schema for dead-letter export path`
- `refactor: extract validation logic into dedicated helper functions`

---

## Engineering & Coding Standards

All submitted code must satisfy these rigorous standards:

1. **Idiomatic Go & Formatting:**
   - Always run `make fmt` (gofmt + goimports) and `make lint` before committing; CI fails on any lint finding.
   - Code must pass `go vet ./...` without any warnings.
   - All exported types, functions, and methods must have clean, complete docstrings.

2. **Low Cyclomatic Complexity & Short Functions:**
   - Functions must not exceed **40 lines of code**.
   - Cyclomatic complexity must remain **$\le 10$** (our CI lint gate strictly flags functions $> 15$).
   - Deconstruct complex branching into focused helper functions and subtests.

3. **Zero Panics & Explicit Error Handling:**
   - Never use `panic()` in production code. Always return errors explicitly.
   - Wrap errors with contextual information using `fmt.Errorf("action %s: %w", item, err)`.
   - Never ignore error returns; use `errcheck` conventions.

4. **Concurrency & Thread Safety:**
   - Concurrency must use bounded channels and worker pools to avoid unbounded memory allocations.
   - Shared mutable state must be guarded with synchronization primitives (`sync.Mutex`, `sync.RWMutex`, or atomic counters).
   - **Zero Tolerance for Data Races:** Every change must pass `go test -race -count=1 ./...`.

5. **Structured Logging:**
   - Use standard Go `log/slog` for all log outputs.
   - Never use `fmt.Println` or unformatted `log.Println` in production code.
   - Include contextual key-value pairs (e.g., `"job_id"`, `"stage"`, `"duration_ms"`).

---

## Testing & Race Detection Standards

We maintain a strict 100% test pass rate with mandatory race condition verification.

```bash
# Run all unit and integration tests:
make test

# Run tests with the Go race detector (mandatory before PR submission):
make race

# Run static analysis and linting:
make vet
make check

# Run load and throughput benchmarks:
make load-test
```

### Writing New Tests
- Use Go's standard `testing` package with `t.Helper()` for shared assertion functions.
- Separate subtests cleanly using `t.Run("SubTestName", func(t *testing.T) { ... })`.
- Safely check type assertions using the comma-ok idiom (`val, ok := obj.(Type)`) to satisfy `errcheck`.

---

## Submitting a Pull Request

1. **Keep PRs Focused:** Submit small, targeted pull requests addressing a single issue or feature.
2. **Run Local Verification:**
   ```bash
   make check && make build
   ```
3. **Fill Out the PR Template:** Complete all sections of the [Pull Request Template](.github/pull_request_template.md).
4. **CI Verification:** Ensure all automated GitHub Actions checks pass:
   - Code Formatting (`gofmt`)
   - Static Analysis (`golangci-lint` incl. gosec) and `govulncheck`
   - Race tests with a 75% coverage gate
   - Container build, smoke test and Trivy scan
   - Race Condition Test Suite (`go test -race`)
   - Docker Container Image Build

---

## Reporting Bugs & Suggesting Enhancements

- **Found a bug?** Open an issue using the [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md) with reproduction steps and configuration payloads.
- **Have an idea?** Open an issue using the [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md) describing the problem statement and proposed architectural design.

Thank you for helping make the Data Processing Pipeline robust, performant, and reliable!

