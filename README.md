# Concurrent Data Processing Pipeline (Go)

[![Go Report Card](https://goreportcard.com/badge/github.com/anandmindfire/data-processing-pipeline)](https://goreportcard.com/report/github.com/anandmindfire/data-processing-pipeline)
[![CI Pipeline](https://github.com/anandmindfire/data-processing-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/anandmindfire/data-processing-pipeline/actions/workflows/ci.yml)
[![OpenAPI Spec](https://img.shields.io/badge/OpenAPI-3.0.3-blue.svg)](docs/openapi.json)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8.svg)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Zero Data Races](https://img.shields.io/badge/go%20test%20-race-brightgreen.svg)](Makefile)

A robust, production-oriented, concurrent data processing engine and job management REST service implemented in idiomatic Go. It ingests data from heterogeneous sources (CSV files, JSON files, external HTTP APIs), validates and cleans records against schema and business rules, executes parallel transformations, calculates grouped analytical aggregations, and exports results to storage targets — all while supporting real-time progress tracking, error quarantine, cancellation, and automated tests.

---

## Table of Contents

- [Key Architectural Highlights](#key-architectural-highlights)
- [System Architecture](#system-architecture)
  - [High-Level Layered Architecture](#high-level-layered-architecture)
  - [5-Stage Channel Mesh & Data Flow](#5-stage-channel-mesh--data-flow)
- [Technology Stack](#technology-stack)
- [Project Layout (Clean Architecture)](#project-layout-clean-architecture)
- [Quickstart Guide](#quickstart-guide)
  - [Prerequisites](#prerequisites)
  - [Local Setup via Makefile](#local-setup-via-makefile)
  - [Docker & Container Orchestration](#docker--container-orchestration)
  - [Configuration & Environment Variables](#configuration--environment-variables)
- [API Reference](#api-reference)
  - [Endpoint Overview](#endpoint-overview)
  - [Interactive Swagger UI](#interactive-swagger-ui)
  - [Example Requests & Responses](#example-requests--responses)
- [Concurrency & Fault Tolerance](#concurrency--fault-tolerance)
  - [Bounded Channels & Memory Backpressure](#bounded-channels--memory-backpressure)
  - [Worker Pool Fan-Out / Fan-In](#worker-pool-fan-out--fan-in)
  - [Centralized Error Quarantine & Dead-Letter Handling](#centralized-error-quarantine--dead-letter-handling)
  - [Cooperative Cancellation Hierarchy](#cooperative-cancellation-hierarchy)
  - [Transient HTTP Retries with Exponential Backoff](#transient-http-retries-with-exponential-backoff)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [Git Quality Automation & CI/CD](#git-quality-automation--cicd)
- [Documentation Index](#documentation-index)

---

## Key Architectural Highlights

- **Clean Architecture & Separation of Concerns:** Decoupled packages with clear boundaries (`models`, `ingestion`, `validation`, `transformation`, `aggregation`, `export`, `storage`, `jobs`, `pipeline`, `api`, `metrics`, `config`).
- **Bounded Channels & Backpressure:** Pipeline stages communicate strictly via typed, bounded channels (`recordsCh` ➔ `validatedCh` ➔ `transformedCh` ➔ `resultCh`), eliminating unbounded heap growth and providing natural backpressure on disk I/O and network requests.
- **Actor-Style Concurrency (Fan-Out / Fan-In):** Dedicated goroutines per ingestion source stream records in parallel; configurable worker pools parallelize CPU-bound validation and transformation; single-consumer goroutines handle aggregation, export, progress tracking, and error collection without mutex lock contention.
- **Hierarchical Cooperative Cancellation:** Every pipeline job runs under a hierarchical `context.Context` with configurable timeouts. Cancellation via `PATCH /api/v1/pipelines/:id/cancel` propagates instantly to disk readers, HTTP requests, and worker pool loops.
- **Centralized Error Quarantine:** Faulty or corrupted records are isolated with stage and field diagnostic metadata into a centralized error collection over `errorCh`. Valid records continue processing to completion.
- **Dead-Letter Handling:** Configurable destination path (`dead_letter_path`) automatically persists quarantined invalid records and error traces to disk upon completion.
- **Multi-Sink Export Engine:** Pluggable exporter generating indented JSON, RFC 4180 compliant CSV (with deterministic union headers), and transactional SQLite DDL & batch `INSERT` SQL scripts.
- **Reliability & Observability:** Structured logging with standard library `log/slog`, panic recovery middleware, IP-based token-bucket rate limiting, embedded Swagger UI (`/docs`), and Prometheus metrics exposition (`/metrics`).
- **100% Race-Clean & Tested:** Comprehensive unit tests, HTTP mock servers, and end-to-end integration tests passing with zero data races (`go test -race -count=1 ./...`).

---

## System Architecture

### High-Level Layered Architecture

```mermaid
flowchart TD
  subgraph Ingress ["API & Transport Layer (HTTP REST)"]
    direction TB
    CLI["Client / curl / UI"] -->|"HTTP Requests"| MW["Middleware Chain<br/>(Panic Recovery, slog Logger, Token-Bucket Rate Limiter)"]
    MW --> ROUTER["chi v5 Router<br/>/api/v1/pipelines"]
    ROUTER --> DOCS["Swagger UI (/docs)<br/>Prometheus (/metrics)<br/>Health (/healthz)"]
  end

  subgraph ControlPlane ["Job Orchestration & Control Plane"]
    ROUTER -->|"Submit Job"| MGR["jobs.Manager"]
    MGR -->|"Check Quotas & Lifecycle"| QUOTA["Active Job Limit<br/>(MaxActiveJobs)"]
    MGR -->|"Manage Cancellation"| CTX["context.Context Hierarchy<br/>(WithTimeout + WithCancel)"]
  end

  subgraph Engine ["5-Stage Concurrent Pipeline Engine"]
    direction TB
    subgraph S1 ["Stage 1: Ingestion Fan-Out"]
      SRC1["CSV Reader (Local/Remote)"]
      SRC2["JSON Reader (Streaming)"]
      SRC3["HTTP Reader (3x Retry + Backoff)"]
    end

    subgraph Buffers ["Bounded Channel Mesh & Telemetry"]
      R_CH[("recordsCh<br/>(Bounded Backpressure)")]
      V_CH[("validatedCh<br/>(Bounded)")]
      T_CH[("transformedCh<br/>(Bounded)")]
      RES_CH[("resultCh<br/>(Bounded)")]
      ERR_CH[("errorCh<br/>(Quarantine)")]
      PROG_CH[("progressCh<br/>(Telemetry Stream)")]
    end

    subgraph S2 ["Stage 2: Validation Fan-Out"]
      VAL_POOL["Validation Worker Pool<br/>(Type, Bounds, Pattern Caching)"]
    end

    subgraph S3 ["Stage 3: Transformation Fan-Out"]
      TR_POOL["Transformation Worker Pool<br/>(Normalization, Math, Fallbacks)"]
    end

    subgraph S4 ["Stage 4: Aggregation Fan-In"]
      AGG["Single-Consumer Aggregator<br/>(Group-by Count, Sum, Avg, Min, Max)"]
    end

    subgraph S5 ["Stage 5: Multi-Sink Export"]
      EXP["Export Dispatcher<br/>(JSON, CSV, SQLite DDL/INSERT, Dead-Letter)"]
    end

    S1 --> R_CH
    R_CH --> VAL_POOL
    VAL_POOL -->|"Valid Records"| V_CH
    VAL_POOL -->|"Invalid Records"| ERR_CH
    VAL_POOL -.->|"Latency"| PROG_CH

    V_CH --> TR_POOL
    TR_POOL -->|"Transformed Records"| T_CH
    TR_POOL -->|"Transform Errors"| ERR_CH
    TR_POOL -.->|"Latency"| PROG_CH

    T_CH --> AGG
    AGG -->|"Stream Record"| RES_CH
    AGG -.->|"Latency"| PROG_CH

    RES_CH --> EXP
    EXP -.->|"Export Stats"| PROG_CH
  end

  subgraph Persistence ["Persistence & Observability Layer"]
    ERR_CH -->|"Drain Errors"| ERR_COL["Error Collector Goroutine"]
    PROG_CH -->|"Drain Telemetry"| PROG_COL["Progress Tracker Goroutine"]
    EXP --> REPO[("storage.Repository<br/>(Atomic File / In-Memory / PostgreSQL)")]
    ERR_COL --> REPO
    PROG_COL --> METRICS["Prometheus Metrics Collector<br/>(GlobalCollector)"]
    PROG_COL -.-> REPO
  end
```

### 5-Stage Channel Mesh & Data Flow

1. **Stage 1 (Ingestion Fan-Out):** Each input source runs in an independent goroutine, streaming parsed records (`ingestion.Item`) into a bounded channel (`rawCh`). If a remote HTTP source experiences a 5xx error or transient network drop, it automatically retries up to 3 times with exponential backoff.
2. **Stage 2 (Validation Fan-Out):** A configurable pool of validation workers (`ValidationWorkers`) checks required fields, data types, numeric boundaries, and regex patterns (using cached precompiled expressions). Valid records move to `validCh`; invalid rows are diverted to `errCh`.
3. **Stage 3 (Transformation Fan-Out):** A configurable pool of transformation workers (`TransformWorkers`) applies casing normalization, trimming, mathematical scaling, offsets, and fallback default values. Output records stream to `transformedCh`.
4. **Stage 4 (Aggregation Fan-In):** A single-consumer goroutine drains `transformedCh` and updates grouped accumulators (`count`, `sum`, `avg`, `min`, `max`). Single ownership eliminates mutex contention on the hot path.
5. **Stage 5 (Multi-Sink Export):** Transformed records and computed summary aggregates are written to specified targets (JSON, CSV, SQLite DDL/INSERT batch, and Dead-Letter queue files).

---

## Technology Stack

| Component | Technology | Rationale & Industry Standard Alignment |
| :--- | :--- | :--- |
| **Language** | **Go 1.23+** | Idiomatic Go, memory efficiency, native goroutines, and first-class channel concurrency. |
| **REST API Transport** | **`net/http`** | Go standard library HTTP server with production-grade read/write/idle timeouts. |
| **HTTP Routing** | **`github.com/go-chi/chi/v5`** | Zero-allocation lightweight router supporting URL parameter routing and composable middleware. |
| **Logging** | **`log/slog`** | Standard library structured, contextual JSON/text logging. |
| **Metrics & Telemetry** | **Prometheus** | Industry standard exposition format at `/metrics` (job counters, rate gauges, stage latency distributions). |
| **Documentation** | **OpenAPI 3.0.3 & Swagger UI** | Zero-dependency embedded Swagger UI served at `/docs` with interactive API execution. |
| **Persistence** | **Repository Pattern (`database/sql` Ready)** | Abstracted persistence via `storage.Repository` interface with atomic file and in-memory implementations. |
| **Export Formats** | **JSON, CSV, SQLite SQL** | Indented JSON, RFC 4180 CSV, and transactional SQLite scripts (`CREATE TABLE` + batch `INSERT`). |
| **Containerization** | **Docker & Distroless** | Multi-stage build yielding minimal, secure, non-root Debian 12 distroless container images. |
| **CI / Automation** | **GitHub Actions** | Automated CI pipeline checking formatting, static analysis (`go vet`), race testing, and container builds. |

---

## Project Layout (Clean Architecture)

```
data-processing-pipeline/
├── cmd/
│   └── server/
│       └── main.go                  # Composition root, dependency injection & OS signal shutdown
├── internal/
│   ├── aggregation/
│   │   ├── aggregator.go            # Group-by count, sum, average, min, max engine
│   │   ├── aggregator_test.go       # Aggregation unit tests
│   │   ├── errors.go                # Aggregation domain errors
│   │   └── types.go                 # Aggregator interfaces and state types
│   ├── api/
│   │   ├── middleware/
│   │   │   ├── logging.go           # Structured HTTP request logger (`log/slog`)
│   │   │   ├── middleware_test.go   # Middleware unit tests
│   │   │   ├── ratelimit.go         # Token-bucket rate limiter per client IP
│   │   │   └── recovery.go          # HTTP panic recovery middleware
│   │   ├── docs.go                  # Embedded Swagger UI handler & OpenAPI spec server
│   │   ├── handler.go               # HTTP handlers for all 8 REST endpoints
│   │   ├── openapi.json             # Embedded OpenAPI 3.0.3 specification
│   │   ├── response.go              # Standard JSON response and error envelopes
│   │   ├── routes.go                # Route multiplexer and middleware decorators (chi v5)
│   │   ├── server.go                # Server lifecycle and configuration
│   │   ├── server_test.go           # REST API endpoint unit & mock tests
│   │   └── validation.go            # Upfront job request specification validator
│   ├── config/
│   │   ├── config.go                # Strongly-typed configuration with environment overrides
│   │   └── config_test.go           # Configuration tests
│   ├── export/
│   │   ├── csv.go                   # RFC 4180 CSV writer with deterministic union headers
│   │   ├── errors.go                # Export domain errors
│   │   ├── exporter.go              # Exporter dispatcher and target coordinator
│   │   ├── exporter_test.go         # Exporter unit tests
│   │   ├── json.go                  # Formatted JSON record writer
│   │   └── sqlite.go                # Transactional SQLite DDL & batch INSERT generator
│   ├── ingestion/
│   │   ├── csv.go                   # Streaming CSV reader
│   │   ├── errors.go                # Ingestion domain errors
│   │   ├── http.go                  # HTTP client with 3x exponential backoff retry
│   │   ├── json.go                  # Streaming JSON reader (arrays and objects)
│   │   ├── reader.go                # Unified multi-source stream dispatcher
│   │   ├── reader_test.go           # Ingestion unit and mock tests
│   │   └── types.go                 # Ingested item definition
│   ├── jobs/
│   │   ├── errors.go                # Domain error definitions (capacity, not found, conflicts)
│   │   ├── manager.go               # Job manager, active quotas, and async orchestration
│   │   ├── manager_test.go          # Job manager unit tests
│   │   └── service.go               # Progress, error querying, results, and cancellation logic
│   ├── metrics/
│   │   ├── collector.go             # Telemetry collector (counters, latencies, rates)
│   │   ├── metrics_test.go          # Metrics unit tests
│   │   └── prometheus.go            # Prometheus text format serializer
│   ├── models/
│   │   ├── config.go                # Config, Source, Rule, Aggregation, ExportTarget models
│   │   ├── job.go                   # Job struct and state models
│   │   ├── metrics.go               # Observability and Progress tracking models
│   │   ├── record.go                # Record map and RecordError definitions
│   │   └── status.go                # Job status constants (PENDING, RUNNING, COMPLETED, etc.)
│   ├── pipeline/
│   │   ├── engine.go                # Direct pipeline coordinator
│   │   ├── engine_test.go           # Pipeline engine tests
│   │   ├── orchestrator.go          # Full multi-stage channel mesh execution loop
│   │   ├── pipeline.go              # Pipeline struct constructor and interfaces
│   │   ├── pipeline_test.go         # Direct pipeline execution tests
│   │   └── worker.go                # Validation and transformation worker pools
│   └── storage/
│       ├── errors.go                # Storage domain errors
│       ├── file.go                  # Atomic JSON file repository (crash-safe snapshot storage)
│       ├── memory.go                # Thread-safe in-memory repository (testing)
│       ├── repository.go            # Storage interface definition
│       ├── storage_test.go          # Storage persistence tests
│       └── store.go                 # Backward-compatibility storage adapter
├── test/
│   └── pipeline_integration_test.go # End-to-end integration tests (CSV + JSON + HTTP)
├── configs/
│   └── default.json                 # Sample server configuration
├── docs/
│   ├── design-report.md             # <= 1-page design report (architecture & concurrency model)
│   ├── openapi.json                 # Standalone OpenAPI 3.0.3 specification
│   └── production-readiness.md      # Detailed comparison: code quality vs. cloud production readiness
├── samples/
│   ├── input.csv                    # Sample CSV input file
│   ├── input.json                   # Sample JSON input file
│   ├── job.json                     # Complete sample pipeline job specification
│   ├── mixed_job.json               # Sample mixed job (CSV file + JSON file + HTTP REST API)
│   └── responses/                   # Example API JSON responses
├── .github/                         # GitHub repository configuration
│   ├── workflows/
│   │   └── ci.yml                   # GitHub Actions CI (gofmt, vet, race tests, Docker build)
│   └── pull_request_template.md     # Standardized Pull Request template
├── .githooks/                       # Native git hooks (pre-commit, commit-msg)
│   ├── commit-msg                   # Conventional Commits format validator
│   └── pre-commit                   # Formatter (gofmt/gofumpt), vet, and race tests runner
├── .golangci.yml                    # golangci-lint linter configuration (ESLint equivalent)
├── .lefthook.yml                    # Lefthook git hook configuration (Husky equivalent)
├── Dockerfile                       # Multi-stage distroless container build
├── docker-compose.yml               # Container orchestration with volumes & healthchecks
├── Makefile                         # Automation targets for build, test, race, vet, and run
├── go.mod                           # Go module definition
└── go.sum                           # Checksums for external dependencies
```

---

## Quickstart Guide

### Prerequisites
- **Go 1.23+** (or Docker)
- **Make** (optional, recommended)

### Local Setup via Makefile

```bash
# 1. Format code, run static analysis (go vet), and execute tests with race detector
make check

# 2. Build the optimized server binary
make build

# 3. Start the server locally on :8080
make run
```

### Docker & Container Orchestration

Run the pipeline service in an isolated, multi-stage distroless container:

```bash
# Build the container image
make docker-build

# Launch the container stack with Docker Compose
docker compose up -d

# View live application logs
docker compose logs -f pipeline

# Stop the stack
docker compose down
```

### Configuration & Environment Variables

The server implements 12-factor configuration loading default JSON configurations overridden by environment variables:

| Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` / `SERVER_PORT` | string | `8080` | Port for the HTTP server to listen on |
| `ADDR` / `SERVER_ADDR` | string | `:8080` | Host and port address string |
| `DATA_DIR` | string | `data` | Directory where persistent job state is saved |
| `EXPORT_DIR` | string | `exports` | Directory for generated CSV/JSON/SQL output files |
| `MAX_ACTIVE_JOBS` | int | `10` | Maximum number of concurrent running jobs |
| `RATE_LIMIT_RPS` | float | `50.0` | Token bucket refill rate per second per IP |
| `RATE_LIMIT_BURST` | int | `100` | Maximum token bucket burst capacity per IP |
| `DEFAULT_TIMEOUT_SECONDS` | int | `300` | Default job timeout (overridable per job) |
| `LOG_LEVEL` | string | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `LOG_FORMAT` | string | `text` | Structured log format (`json` or `text`) |
| `VALIDATION_WORKERS` | int | `4` | Default worker pool size for validation stage |
| `TRANSFORM_WORKERS` | int | `4` | Default worker pool size for transformation stage |
| `BUFFER_SIZE` | int | `64` | Channel capacity for inter-stage buffers |

---

## API Reference

### Endpoint Overview

| Method | Endpoint | Status | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/pipelines` | `202 Accepted` | Submit and trigger a new pipeline execution job |
| `GET` | `/api/v1/pipelines` | `200 OK` | List all submitted pipeline jobs and summary statuses |
| `GET` | `/api/v1/pipelines/{id}` | `200 OK` | Retrieve detailed configuration and metrics for a job |
| `GET` | `/api/v1/pipelines/{id}/progress` | `200 OK` | Poll real-time progress, processed counts, and stage latencies |
| `GET` | `/api/v1/pipelines/{id}/results` | `200 OK` | Retrieve processed records, computed aggregations, and export paths |
| `GET` | `/api/v1/pipelines/{id}/errors` | `200 OK` | Inspect quarantined errors and failed record details |
| `PATCH`| `/api/v1/pipelines/{id}/cancel` | `202 Accepted` | Trigger cooperative cancellation on a running job |
| `DELETE`| `/api/v1/pipelines/{id}` | `204 No Content` | Delete a finished job, state entries, and export files |
| `GET` | `/docs` | `200 OK` | Interactive embedded Swagger UI API documentation |
| `GET` | `/docs/openapi.json` | `200 OK` | OpenAPI 3.0.3 specification JSON |
| `GET` | `/metrics` | `200 OK` | Prometheus text exposition metrics endpoint |
| `GET` | `/healthz` | `200 OK` | Liveness and readiness health probe |

### Interactive Swagger UI

Open your browser to explore and execute API calls directly:
```
http://localhost:8080/docs
```

---

### Example Requests & Responses

#### 1. Submit a Pipeline Job
```bash
curl -sS -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d @samples/job.json
```

**Response (`202 Accepted`):**
```json
{
  "id": "20261104T100000.000000000-001",
  "status": "PENDING",
  "created_at": "2026-11-04T10:00:00Z"
}
```

#### 2. Submit a Mixed Source Job (CSV + JSON + HTTP REST API)
```bash
curl -sS -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d @samples/mixed_job.json
```

#### 3. Poll Real-Time Progress & Latencies
```bash
curl -sS http://localhost:8080/api/v1/pipelines/<JOB_ID>/progress
```

**Response (`200 OK`):**
```json
{
  "job_id": "20261104T100000.000000000-001",
  "status": "RUNNING",
  "percent_complete": 85.71,
  "processed": 6,
  "discovered": 7,
  "pending": 1,
  "valid": 6,
  "rejected": 1,
  "error_count": 1,
  "records_per_second": 3.82,
  "stage_latency_ns": {
    "validation": 420000,
    "transformation": 310000,
    "aggregation": 152000
  }
}
```

#### 4. Retrieve Computed Aggregations & Export Details
```bash
curl -sS http://localhost:8080/api/v1/pipelines/<JOB_ID>/results
```

**Response (`200 OK`):**
```json
{
  "job_id": "20261104T100000.000000000-001",
  "status": "COMPLETED",
  "records": [
    {"department": "engineering", "name": "Alice", "salary": 120000},
    {"department": "sales", "name": "Bob", "salary": 90000}
  ],
  "aggregations": {
    "employee_count": {"engineering": 3, "sales": 3},
    "salary_total": {"engineering": 365000, "sales": 265000},
    "salary_average": {"engineering": 121666.67, "sales": 88333.33}
  },
  "export_path": "exports/employees.json"
}
```

#### 5. Inspect Quarantined Errors
```bash
curl -sS http://localhost:8080/api/v1/pipelines/<JOB_ID>/errors
```

**Response (`200 OK`):**
```json
{
  "job_id": "20261104T100000.000000000-001",
  "errors": [
    {
      "source": "employees-csv",
      "index": 4,
      "stage": "validation",
      "message": "field \"salary\": invalid number \"invalid\"",
      "record": {
        "department": "Engineering",
        "name": "Eve",
        "salary": "invalid"
      },
      "at": "2026-11-04T10:00:00.120Z"
    }
  ]
}
```

#### 6. Cancel a Running Job
```bash
curl -sS -X PATCH http://localhost:8080/api/v1/pipelines/<JOB_ID>/cancel
```

---

## Concurrency & Fault Tolerance

### Bounded Channels & Memory Backpressure
All inter-stage communication occurs across typed, bounded Go channels configured by `BufferSize`. When worker stages experience CPU spikes or downstream consumers slow down, upstream stages automatically block on send, creating natural backpressure. Memory consumption remains strictly bounded regardless of dataset scale.

### Worker Pool Fan-Out / Fan-In
Validation and Transformation execute concurrently using parameterized worker pools. Workers consume records from input channels, process them concurrently, and forward valid items downstream. Aggregation employs single-consumer fan-in, ensuring thread-safe accumulator updates without mutex locking overhead on the critical path.

### Centralized Error Quarantine & Dead-Letter Handling
A single corrupted record (e.g. malformed type, regex mismatch, or numeric out-of-bounds) never aborts pipeline execution. Bad rows are decorated with stage diagnostics and emitted to `errorCh`. When `dead_letter_path` is specified in the job configuration, the quarantined records and stack metadata are exported to disk automatically.

### Cooperative Cancellation Hierarchy
Every job is bound to a hierarchical context:
```go
ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
defer cancel()
```
Cancellation triggers immediately via API (`PATCH /api/v1/pipelines/:id/cancel`) or timeout. Readers, HTTP requests (`http.NewRequestWithContext`), and worker pool `select` blocks terminate cooperatively, releasing file descriptors and goroutines cleanly. Terminal states are guaranteed persistence using dedicated non-cancelled contexts.

### Transient HTTP Retries with Exponential Backoff
Remote HTTP sources incorporate automatic retry logic for transient failures (e.g., connection resets or 5xx HTTP responses):
- Maximum 3 attempts with exponential backoff ($50\text{ms} \rightarrow 100\text{ms} \rightarrow 200\text{ms}$).
- Client errors (4xx) fail immediately without wasteful retries.
- Respects `ctx.Done()` during backoff intervals to allow cancellation.

---

## Testing & Quality Assurance

The codebase includes comprehensive test suites spanning parsers, validators, transformers, aggregators, exporters, mock transports, API handlers, and end-to-end integration flows:

```bash
# Run all tests across packages
go test -v ./...

# Verify zero race conditions using Go's race detector
go test -race -count=1 ./...

# Run static analysis
go vet ./...

# Run complete verification suite (fmt + vet + race)
make check
```

---

## Git Quality Automation & CI/CD

### Native Git Hooks Setup
This repository includes a native Go git tooling setup (the Go equivalent of Husky, Prettier, ESLint, and commitlint):

| Standard Tooling | Go Equivalent in this Repo | Configuration File |
| :--- | :--- | :--- |
| **Husky** | Native Git Hooks / **`lefthook`** | `.githooks/` & `.lefthook.yml` |
| **Prettier** | **`gofmt`** / **`gofumpt`** | Executed in pre-commit hook & `make fmt` |
| **ESLint** | **`golangci-lint`** / `go vet` | `.golangci.yml` & `make lint` |
| **commitlint** | Conventional Commits hook | `.githooks/commit-msg` |

To activate automated commit checks locally:
```bash
make hooks
```

### Continuous Integration (GitHub Actions)
Automated CI executes on every push and pull request via [`.github/workflows/ci.yml`](.github/workflows/ci.yml):
1. **Formatting & Static Analysis:** Checks `gofmt` compliance and executes `go vet` and `golangci-lint`.
2. **Race Condition Tests:** Runs `go test -race -v -covermode=atomic ./...` across all 15 packages.
3. **Binary & Container Build Verification:** Compiles `bin/pipeline-server` and builds the multi-stage distroless Docker image.

### Pull Request Template
A standardized Pull Request template is configured at [`.github/pull_request_template.md`](.github/pull_request_template.md) covering change classification, pipeline component checklist, verification steps, and breaking change disclosures.

---

## Documentation Index

- **[Design Report (<= 1 Page)](docs/design-report.md):** Executive report summarizing design choices, the concurrency model, channel lifecycle ownership, and engineering trade-offs.
- **[Production Readiness Analysis](docs/production-readiness.md):** In-depth engineering blueprint contrasting current codebase achievements with enterprise cloud requirements (PostgreSQL migrations, S3/GCS object storage, durable queues, OpenTelemetry, and container hardening).
- **[OpenAPI 3.0.3 Specification](docs/openapi.json):** Full OpenAPI specification describing all REST endpoints, parameters, schemas, and responses.
