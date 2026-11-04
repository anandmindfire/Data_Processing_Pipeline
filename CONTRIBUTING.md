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
├── cmd/
│   └── server/                     # Application entry point (main.go & CLI probes)
├── configs/                        # 12-factor configuration & environment loaders
├── internal/
│   ├── api/                        # HTTP transport layer (controllers, routes, middleware, dto, validator)
│   ├── bootstrap/                  # Dependency injection and application lifecycle orchestrator
│   ├── pipeline/                   # Core pipeline engine, channels, coordinator, workers, error collector
│   ├── ingestion/                  # CSV, JSON, and SSRF-protected HTTP stream readers
│   ├── validation/                 # Schema bounds checking and business validation
│   ├── transformation/             # Normalization, type mapping, and enrichment
│   ├── aggregation/                # In-memory metrics & group-by aggregation engine
│   ├── export/                     # Multi-target export engine (CSV, JSON, SQLite, DLQ)
│   ├── storage/                    # Durable persistence (in-memory, file-based, SQLite)
│   ├── repositories/               # Repository interfaces and SQLite implementations
│   └── models/                     # Canonical domain models (Job, Record, Config, Rule)
├── pkg/                            # Reusable shared utilities (logger, response, errors, constants)
├── test/
│   ├── integration/                # End-to-end integration and API lifecycle tests
│   ├── unit/                       # Component-level unit test suites
│   ├── mocks/                      # Mock transports and readers
│   └── testdata/                   # Fixture CSV and JSON files
├── scripts/                        # Load testing, benchmark, and runner scripts
└── .github/                        # CI workflows, PR templates, and issue templates
```

---

## Prerequisites & Local Setup

### Prerequisites
- **Go:** `1.23+` (recommended: Go 1.23 or latest stable)
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
   - Always run `make fmt` (`gofmt -s -w .`) before committing.
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
   - Static Analysis (`go vet` & `golangci-lint`)
   - Race Condition Test Suite (`go test -race`)
   - Docker Container Image Build

---

## Reporting Bugs & Suggesting Enhancements

- **Found a bug?** Open an issue using the [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md) with reproduction steps and configuration payloads.
- **Have an idea?** Open an issue using the [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md) describing the problem statement and proposed architectural design.

Thank you for helping make the Data Processing Pipeline robust, performant, and reliable!

