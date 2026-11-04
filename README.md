# Concurrent Data Processing Pipeline (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/anandmindfire/data-processing-pipeline.svg)](https://pkg.go.dev/github.com/anandmindfire/data-processing-pipeline)
[![CI Pipeline](https://github.com/anandmindfire/Data_Processing_Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/anandmindfire/Data_Processing_Pipeline/actions/workflows/ci.yml)
[![OpenAPI Spec](https://img.shields.io/badge/OpenAPI-3.0.3-blue.svg)](internal/api/openapi.json)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8.svg)](go.mod)
[![Code Quality](https://img.shields.io/badge/code%20quality-golangci--lint-brightgreen.svg)](.golangci.yml)
[![Zero Data Races](https://img.shields.io/badge/go%20test%20-race-brightgreen.svg)](Makefile)

A robust, production-oriented, concurrent data processing engine and job management REST service implemented in idiomatic Go. It ingests data from heterogeneous sources (CSV files, JSON files, external HTTP APIs), validates and cleans records against schema and business rules, executes parallel transformations, calculates grouped analytical aggregations, and exports results to storage targets — all while supporting real-time progress tracking, error quarantine, cancellation, and automated tests.

---

## Table of Contents

- [Key Architectural Highlights](#key-architectural-highlights)
- [System Architecture](#system-architecture)
  - [How Data Moves Through the Pipeline](#how-data-moves-through-the-pipeline)
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
- [Continuous Integration (CI/CD)](#continuous-integration-cicd)
- [Contributing & Community](#contributing--community)
- [Documentation Index](#documentation-index)

---

## Key Architectural Highlights

- **Clean Architecture & Separation of Concerns:** Decoupled packages with clear boundaries (`models`, `ingestion`, `validation`, `transformation`, `aggregation`, `export`, `storage`, `jobs`, `pipeline`, `api`, `metrics`, `config`).
- **Bounded Channels & Backpressure:** Pipeline stages communicate strictly via typed, bounded channels (`recordsCh` ➔ `validatedCh` ➔ `transformedCh` ➔ `resultCh`), eliminating unbounded heap growth and providing natural backpressure on disk I/O and network requests.
- **Actor-Style Concurrency (Fan-Out / Fan-In):** Dedicated goroutines per ingestion source stream records in parallel; configurable worker pools parallelize CPU-bound validation and transformation; single-consumer goroutines handle aggregation, export, progress tracking, and error collection without mutex lock contention.
- **Hierarchical Cooperative Cancellation:** Every pipeline job runs under a hierarchical `context.Context` with configurable timeouts. Cancellation via `PATCH /api/v1/pipelines/:id/cancel` propagates instantly to disk readers, HTTP requests, and worker pool loops.
- **Centralized Error Quarantine:** Faulty or corrupted records are isolated with stage and field diagnostic metadata into a centralized error collection over `errorCh`. Valid records continue processing to completion.
- **Dead-Letter Handling:** Configurable destination path (`dlq_path`) persists quarantined invalid records and error traces to disk upon completion.
- **Streaming Multi-Sink Export:** A dedicated export goroutine streams record batches to every target while the job runs: JSON, RFC 4180 CSV (deterministic union headers), a real **SQLite database** (`records` + `aggregations` tables, via `database/sql`), and a portable SQL dump. Files are written to `<path>.part` and atomically renamed only when the job completes, so cancelled or failed runs never publish partial output.
- **Durable Job Store:** Job metadata, processed records, and record errors are persisted in an embedded SQLite database (`DATA_DIR/pipeline.db`, WAL mode, versioned migrations) with batched transactional writes.
- **Live Progress:** The progress tracker persists a metrics snapshot every 500 ms while a job runs, so `GET /progress` reports real-time counts, throughput, and stage latencies.
- **Secure by Default:** Local source paths are sandboxed to `INPUT_DIRS`, every export/DLQ path to `EXPORT_DIR` (symlink-aware); HTTP sources are SSRF-protected at dial time (private, loopback, link-local and metadata ranges blocked, redirects re-validated); per-job worker, buffer, batch, timeout, and source limits are enforced at submission.
- **Lifecycle Safety:** `SIGTERM` stops intake, cancels running jobs (recorded as `CANCELLED: server shutting down`), and drains them before exit; jobs interrupted by a crash are marked `FAILED` on the next start.
- **Reliability & Observability:** Structured logging with standard library `log/slog`, panic recovery middleware, IP-based token-bucket rate limiting, embedded Swagger UI (`/docs`), and Prometheus metrics exposition (`/metrics`).
- **100% Race-Clean & Tested:** Comprehensive unit tests, HTTP mock servers, and end-to-end integration tests passing with zero data races (`go test -race -count=1 ./...`).

---

## System Architecture

The pipeline processes data using an asynchronous, stage-by-stage architecture connected by bounded Go channels. A client submits a job via the REST API; the Job Manager coordinates lifecycle execution under a cancellation context; and independent worker pools process records concurrently with natural backpressure and error quarantine:

```mermaid
flowchart LR
    CLIENT["Client / HTTP API"] -->|"POST /api/v1/pipelines"| API["chi REST API &<br/>Job Manager"]
    API -->|"Start Job (context)"| INGEST

    subgraph PIPELINE ["Concurrent Pipeline Engine (Goroutines + Bounded Channels)"]
        direction LR
        INGEST["1. Ingestion<br/>(CSV, JSON, HTTP)"] -->|"recordsCh"| VAL["2. Validation<br/>(Worker Pool)"]
        VAL -->|"validatedCh"| TRANS["3. Transformation<br/>(Worker Pool)"]
        TRANS -->|"transformedCh"| AGG["4. Aggregation<br/>(Group-by Stats)"]
        AGG -->|"resultCh"| EXP["5. Export Stage"]
    end

    EXP -->|"Stream batches"| SINKS[("Export Targets<br/>JSON / CSV / SQLite DB / SQL")]
    EXP -->|"Batched writes"| STORE[("Job Store<br/>SQLite pipeline.db")]
    VAL -.->|"invalid records"| DLQ[("Errors &<br/>Dead-Letter File")]
    TRANS -.->|"errors"| DLQ
    PIPELINE -.->|"telemetry"| METRICS["Prometheus /metrics &<br/>Progress Telemetry"]
```

### How Data Moves Through the Pipeline

1. **Ingestion (Fan-Out):** Concurrent readers stream data from CSV files, JSON files, or HTTP APIs into a bounded channel (`recordsCh`). HTTP requests automatically retry transient failures up to 3 times with exponential backoff.
2. **Validation (Worker Pool):** Parallel workers validate records against schema and business rules (types, min/max, regex). Invalid rows are diverted to `errorCh` without stopping the pipeline.
3. **Transformation (Worker Pool):** Parallel workers normalize strings, apply mathematical scaling, and set fallback default values.
4. **Aggregation (Fan-In):** A single consumer accumulates group-by statistics (`count`, `sum`, `avg`, `min`, `max`), eliminating lock contention on the hot path.
5. **Export (Streaming, Multi-Sink):** A single export goroutine batches records (`batch_size`, default 500), persists each batch to the job store in one transaction, and streams it to every export target. Aggregations are written into the SQLite database / SQL dump, and as a `<name>.aggregations.json` sidecar for JSON/CSV exports. Quarantined records can be written to a dead-letter file (`dlq_path`).
6. **Side channels:** `errorCh` feeds one error-collector goroutine (batched persistence); `progressCh` feeds one progress-tracker goroutine that owns the job metrics and persists live snapshots.

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
| **Persistence** | **SQLite via `database/sql` (`modernc.org/sqlite`, pure Go)** | `storage.Repository` with an SQLite implementation (production) and an in-memory implementation (tests). No CGO, so the binary stays static. |
| **Export Formats** | **JSON, CSV, SQLite DB, SQL dump** | Streamed JSON array, RFC 4180 CSV, SQLite database file (`records`, `aggregations`), and portable SQL dump. |
| **Containerization** | **Docker & Distroless** | Multi-stage build yielding minimal, secure, non-root Debian 12 distroless container images. |
| **CI / Automation** | **GitHub Actions** | Automated CI pipeline checking formatting, static analysis (`go vet`), race testing, and container builds. |

---

## Project Layout (Clean Architecture)

```
pipeline-system/
├── cmd/
│   └── server/
│       └── main.go                 # Application entry point & graceful shutdown
│
├── configs/
│   ├── config.go                   # Config struct & validation
│   ├── env.go                      # Environment loader (.env & OS env)
│   ├── default.go                  # Default runtime values
│   └── default.json                # Sample JSON configuration
│
├── internal/
│   ├── api/
│   │   ├── controllers/
│   │   │   └── pipeline_controller.go # REST handlers (CRUD, progress, cancel, metrics)
│   │   ├── routes/
│   │   │   └── routes.go           # Route definitions & URL dispatching
│   │   ├── middleware/
│   │   │   ├── logger.go           # Structured HTTP logger (slog)
│   │   │   ├── recovery.go         # Panic recovery middleware
│   │   │   ├── request_id.go       # Unique request tracing (X-Request-ID)
│   │   │   ├── cors.go             # Cross-Origin Resource Sharing
│   │   │   ├── timeout.go          # Request deadline management
│   │   │   └── ratelimit.go        # Per-IP token-bucket rate limiter
│   │   ├── dto/
│   │   │   ├── request/
│   │   │   │   ├── create_pipeline_request.go
│   │   │   │   └── cancel_pipeline_request.go
│   │   │   └── response/
│   │   │       ├── pipeline_response.go
│   │   │       ├── progress_response.go
│   │   │       ├── error_response.go
│   │   │       └── common_response.go
│   │   └── validator/
│   │       ├── request_validator.go # Spec, limits & enum validation
│   │       └── paths.go            # Input/export path sandboxing
│   │
│   ├── services/
│   │   ├── pipeline_service.go     # Application service orchestrator
│   │   ├── job_service.go          # Job lifecycle & concurrency control
│   │   ├── metrics_service.go      # Telemetry & progress calculations
│   │   └── export_service.go       # Dataset export & artifact cleanup
│   │
│   ├── storage/
│   │   ├── repository.go           # Repository contract (jobs, records, errors, paging)
│   │   ├── sqlite.go               # SQLite job store (production)
│   │   └── memory.go               # In-memory store (tests)
│   │
│   ├── pipeline/
│   │   ├── engine.go               # Pipeline orchestrator & executor
│   │   ├── worker_pool.go          # Concurrent worker pool implementation
│   │   ├── channels.go             # Bounded channel mesh topology
│   │   ├── context.go              # Timeout & cancellation context
│   │   ├── coordinator.go          # Stage-to-stage orchestration
│   │   ├── progress_tracker.go     # Real-time progress tracking
│   │   ├── error_collector.go      # Centralized error & dead-letter handler
│   │   └── metrics.go              # Stage latencies & throughput metrics
│   │
│   ├── ingestion/
│   │   ├── reader.go               # Reader interface & orchestrator
│   │   ├── csv_reader.go           # Streaming CSV reader
│   │   ├── json_reader.go          # Streaming JSON & NDJSON reader
│   │   ├── api_reader.go           # Remote HTTP/API reader with retries
│   │   ├── ssrf.go                 # SSRF-safe HTTP client (dial-time IP checks)
│   │   └── factory.go              # Stream parser factory
│   │
│   ├── validation/
│   │   ├── validator.go            # Validator interface & coordination
│   │   ├── schema_validator.go     # Type checking & required fields
│   │   └── business_validator.go   # Min/max thresholds & regex validation
│   │
│   ├── transformation/
│   │   ├── transformer.go          # Transformer interface & engine
│   │   ├── normalize.go            # String casing & trimming
│   │   ├── enrichment.go           # Math scaling & fallback injection
│   │   └── mapping.go              # Field mapping & record cloning
│   │
│   ├── aggregation/
│   │   ├── aggregator.go           # Streaming aggregation engine
│   │   ├── statistics.go           # Count, sum, average, min, max stats
│   │   ├── groupby.go              # Group-by key resolution
│   │   └── summary.go              # Aggregation finalization & formatting
│   │
│   ├── export/
│   │   ├── exporter.go             # Streaming export session & atomic publish
│   │   ├── file_sinks.go           # JSON, CSV & SQL-dump sinks
│   │   └── sqlite_sink.go          # SQLite database sink
│   │
│   ├── models/
│   │   ├── job.go                  # Job entity & lifecycle states
│   │   ├── record.go               # Record map dictionary
│   │   ├── error.go                # RecordError domain model
│   │   ├── metrics.go              # Metrics & Progress telemetry
│   │   ├── aggregation.go          # Aggregation specifications
│   │   └── pipeline.go             # Pipeline configuration, Source, Rule
│   │
│   ├── database/
│   │   ├── connection.go           # SQL pool & connection manager
│   │   ├── migration.go            # Versioned schema migrations
│   │   └── sqlite.go               # SQLite driver, DSN pragmas (WAL, busy timeout)
│   │
│   └── bootstrap/
│       ├── app.go                  # Application runtime lifecycle
│       ├── dependency.go           # Dependency injection container
│       └── router.go               # HTTP router & middleware chain
│
├── pkg/
│   ├── logger/                     # Global slog logger initialization
│   ├── errors/                     # Standardized error codes & sentinels
│   ├── response/                   # Reusable HTTP JSON response helpers
│   ├── utils/                      # File, pointer, json, csv, time utils
│   ├── constants/                  # Status, headers, stage constants
│   ├── enums/                      # SourceType, ExportType, JobStatus enums
│   ├── interfaces/                 # Core engine contract interfaces
│   └── types/                      # Common types & pagination helpers
│
├── test/
│   ├── integration/                # End-to-end integration tests
│   ├── unit/                       # Component unit tests
│   ├── mocks/                      # Test mocks and stubs
│   └── testdata/                   # Sample datasets (CSV, JSON)
│
├── docs/                           # Architecture reports & OpenAPI spec
├── samples/                        # Pipeline execution examples
├── scripts/                        # Load testing and benchmark scripts
├── .env.example                    # Environment template
├── Makefile                        # Build, test, and verification tasks
├── Dockerfile                      # Distroless container image
├── docker-compose.yml              # Multi-container local stack
├── go.mod                          # Go module dependencies
└── README.md                       # Comprehensive system documentation
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

# Launch the container stack with Docker Compose (host port: ${PORT:-8080})
docker compose up -d

# Put your own input files in ./inputs (mounted read-only at /app/inputs);
# job specs reference them as "/app/inputs/<file>" or the bundled "samples/<file>".

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
| `API_BASE_URL` / `BASE_URL` | string | `http://localhost:8080` | Public base URL shown in the API docs |
| `HEALTHCHECK_URL` | string | local listener | Override the URL probed by `--healthcheck` |
| `API_BASE_PATH` / `API_PREFIX` | string | `/api/v1` | Routing prefix path for pipeline REST endpoints |
| `API_KEY` | string | `""` | Optional secret token for Bearer/X-API-Key authentication |
| `DATA_DIR` | string | `data` | Directory holding the SQLite job store (`pipeline.db`) |
| `EXPORT_DIR` | string | `exports` | Export sandbox: every export and DLQ path must be inside it |
| `INPUT_DIRS` | list | `samples` | Comma-separated directories local file sources may read from |
| `ALLOW_PRIVATE_SOURCES` | bool | `false` | Allow HTTP sources on private/loopback networks (dev only) |
| `MAX_WORKERS_PER_STAGE` | int | `64` | Upper bound for `validation_workers` / `transform_workers` |
| `MAX_BUFFER_SIZE` / `MAX_BATCH_SIZE` | int | `10000` | Upper bounds for `buffer_size` / `batch_size` |
| `MAX_JOB_TIMEOUT_SECONDS` | int | `3600` | Upper bound for `timeout_seconds` |
| `MAX_SOURCES_PER_JOB` / `MAX_EXPORTS_PER_JOB` | int | `16` / `8` | Spec size limits |
| `SHUTDOWN_TIMEOUT` | duration | `25s` | Time to cancel and drain running jobs on `SIGTERM` |
| `READ_TIMEOUT` / `WRITE_TIMEOUT` / `IDLE_TIMEOUT` | duration | `15s` / `30s` / `60s` | HTTP server timeouts |
| `MAX_ACTIVE_JOBS` | int | `10` | Maximum number of concurrent running jobs |
| `RATE_LIMIT_RPS` | float | `50.0` | Token bucket refill rate per second per IP |
| `RATE_LIMIT_BURST` | int | `100` | Maximum token bucket burst capacity per IP |
| `DEFAULT_TIMEOUT_SECONDS` | int | `300` | Default job timeout (overridable per job) |
| `LOG_LEVEL` | string | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `LOG_FORMAT` | string | `text` | Structured log format (`json` or `text`) |
| `DEFAULT_VALIDATION_WORKERS` | int | `4` | Validation pool size when a job does not set one |
| `DEFAULT_TRANSFORM_WORKERS` | int | `4` | Transform pool size when a job does not set one |
| `DEFAULT_BUFFER_SIZE` | int | `64` | Channel capacity when a job does not set one |

Server defaults are applied at submission, so `GET /pipelines/{id}` shows the effective values a job ran with.

---

## API Reference

### Endpoint Overview

| Method | Endpoint | Status | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/pipelines` | `202 Accepted` | Submit and trigger a new pipeline execution job |
| `GET` | `/api/v1/pipelines` | `200 OK` | List jobs, newest first (`?status=&limit=&offset=`, total in `X-Total-Count`) |
| `GET` | `/api/v1/pipelines/{id}` | `200 OK` | Retrieve detailed configuration and metrics for a job |
| `GET` | `/api/v1/pipelines/{id}/progress` | `200 OK` | Poll real-time progress, processed counts, and stage latencies |
| `GET` | `/api/v1/pipelines/{id}/results` | `200 OK` | Aggregations, export paths, and a page of records (`?limit=&offset=`, default 100) |
| `GET` | `/api/v1/pipelines/{id}/errors` | `200 OK` | A page of errors and failed records (`?limit=&offset=`, default 100) |
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
  "id": "20261104T154047.892Z-6b84441b8f46",
  "status": "PENDING",
  "config": { "...": "the validated spec, with canonical paths and effective defaults" },
  "created_at": "2026-11-04T15:40:47.892Z"
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
  "job_id": "20261104T154047.892Z-6b84441b8f46",
  "status": "COMPLETED",
  "percent_complete": 100,
  "processed": 6,
  "discovered": 7,
  "pending": 0,
  "valid": 6,
  "rejected": 1,
  "error_count": 1,
  "started_at": "2026-11-04T21:10:47.893109+05:30",
  "ended_at": "2026-11-04T21:10:47.895992+05:30",
  "records_per_second": 2081.0,
  "stage_latency_ns": {
    "aggregation": 60792,
    "export": 294333,
    "export_finalize": 1186125,
    "ingestion": 395208,
    "transform": 35749,
    "validation": 35541
  }
}
```

> `percent_complete` is `(processed + rejected) / discovered`. Sources are streamed, so the total is not known up front: `discovered` grows while ingestion runs, and the job is only `100%` once its status is `COMPLETED`. When finished, the response also carries `ended_at` and, for failed or cancelled jobs, an `error` reason.

#### 4. Retrieve Computed Aggregations & Export Details
```bash
curl -sS "http://localhost:8080/api/v1/pipelines/<JOB_ID>/results?limit=2&offset=0"
```

**Response (`200 OK`):**
```json
{
  "job_id": "20261104T154047.892Z-6b84441b8f46",
  "status": "COMPLETED",
  "aggregations": {
    "employee_count": {"engineering": 3, "sales": 3},
    "salary_average": {"engineering": 121666.67, "sales": 88333.33},
    "salary_total": {"engineering": 365000, "sales": 265000}
  },
  "export_path": "/app/exports/employees.json",
  "artifacts": [
    "/app/exports/employees.json",
    "/app/exports/employees.aggregations.json",
    "/app/exports/employees.csv"
  ],
  "total_records": 6,
  "offset": 0,
  "limit": 2,
  "records": [
    {"department": "sales", "name": "Dan", "salary": 80000},
    {"department": "engineering", "name": "Alice", "salary": 120000}
  ]
}
```

#### 5. Inspect Quarantined Errors
```bash
curl -sS "http://localhost:8080/api/v1/pipelines/<JOB_ID>/errors?limit=100&offset=0"
```

**Response (`200 OK`):**
```json
{
  "job_id": "20261104T154047.892Z-6b84441b8f46",
  "total": 1,
  "offset": 0,
  "limit": 100,
  "errors": [
    {
      "source": "employees-csv",
      "index": 5,
      "stage": "validation",
      "message": "field \"salary\": invalid number \"invalid\": field type mismatch",
      "record": {"department": "Engineering", "name": "Eve", "salary": "invalid"},
      "at": "2026-11-04T15:40:47.894Z"
    }
  ]
}
```

#### 6. Cancel a Running Job
```bash
curl -sS -X PATCH http://localhost:8080/api/v1/pipelines/<JOB_ID>/cancel
```

The job ends as `CANCELLED` with `"error": "cancelled: cancelled by user request"`. Records processed before the cancel stay queryable through `/results`; export files are not published for cancelled or timed-out jobs.

#### 7. Paths, Limits & Validation Errors

Local sources must be inside `INPUT_DIRS`; export and `dlq_path` targets must be inside `EXPORT_DIR` (relative paths such as `"out.json"` or `"exports/out.json"` resolve there). Every problem in a spec is reported at once with `400 Bad Request`:

```json
{"error": "source[0]: path is outside the permitted directories: \"/etc/hosts\" (allowed: samples)\nvalidation_workers must be <= 64 (got 2000000)"}
```

---

## Concurrency & Fault Tolerance

### Bounded Channels & Memory Backpressure
All inter-stage communication occurs across typed, bounded Go channels configured by `BufferSize`. When worker stages experience CPU spikes or downstream consumers slow down, upstream stages automatically block on send, creating natural backpressure. Memory consumption remains strictly bounded regardless of dataset scale.

### Worker Pool Fan-Out / Fan-In
Validation and Transformation execute concurrently using parameterized worker pools. Workers consume records from input channels, process them concurrently, and forward valid items downstream. Aggregation employs single-consumer fan-in, ensuring thread-safe accumulator updates without mutex locking overhead on the critical path.

### Centralized Error Quarantine & Dead-Letter Handling
A single corrupted record (e.g. malformed type, regex mismatch, or numeric out-of-bounds) never aborts pipeline execution. Bad rows are decorated with stage diagnostics and emitted to `errorCh`. When `dlq_path` is specified in the job configuration, the quarantined records and stack metadata are exported to disk automatically.

### Cooperative Cancellation Hierarchy
Every job is bound to a hierarchical context:
```go
ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
defer cancel()
```
Cancellation triggers immediately via API (`PATCH /api/v1/pipelines/:id/cancel`) or timeout. Readers, HTTP requests (`http.NewRequestWithContext`), and worker pool `select` blocks terminate cooperatively, releasing file descriptors and goroutines cleanly. Terminal states are guaranteed persistence using dedicated non-cancelled contexts.

Each job's root context is created with `context.WithCancelCause`, so the reason is recorded on the job (`error` field): `cancelled by user request`, `server shutting down`, or `job exceeded timeout of Ns`.

### Graceful Shutdown & Crash Recovery
On `SIGINT`/`SIGTERM` the server stops accepting requests, rejects new jobs with `503`, cancels running jobs, and waits up to `SHUTDOWN_TIMEOUT` for them to persist a `CANCELLED` state before closing the store. If the process is killed instead (OOM, `kill -9`), jobs left `PENDING`/`RUNNING` are marked `FAILED` ("interrupted") on the next start, so no job stays `RUNNING` forever. The process exits non-zero if the HTTP listener cannot bind.

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

## Continuous Integration (CI/CD)

Automated continuous integration runs on every push and pull request via [`.github/workflows/ci.yml`](.github/workflows/ci.yml):
1. **Formatting & Static Analysis:** Checks `gofmt` compliance and executes `go vet` and `golangci-lint`.
2. **Race Condition Tests:** Runs `go test -race -v -covermode=atomic ./...` across all packages with zero data races.
3. **Binary & Container Build Verification:** Compiles `bin/pipeline-server` and builds the multi-stage distroless Docker image.

---

## Contributing & Community

We welcome contributions to the Data Processing Pipeline! Please review our community guides:
- **[Contributing Guide](CONTRIBUTING.md):** Complete developer workflow, coding standards, branching strategy, testing guidelines, and PR checklist.
- **[Code of Conduct](CODE_OF_CONDUCT.md):** Contributor Covenant v2.1 community behavior standards.
- **[Pull Request Template](.github/pull_request_template.md):** Standardized pull request submission template.
- **[Issue Templates](.github/ISSUE_TEMPLATE/):** Bug reports and feature request templates.

---

## Documentation Index

- **[Design Report (<= 1 Page)](docs/design-report.md):** Executive report summarizing design choices, the concurrency model, channel lifecycle ownership, and engineering trade-offs.
- **[Production Readiness Analysis](docs/production-readiness.md):** In-depth engineering blueprint contrasting current codebase achievements with enterprise cloud requirements (PostgreSQL migrations, S3/GCS object storage, durable queues, OpenTelemetry, and container hardening).
- **[OpenAPI 3.0.3 Specification](internal/api/openapi.json):** Full OpenAPI specification describing all REST endpoints, parameters, schemas, and responses.
- **[Contributing Guidelines](CONTRIBUTING.md):** Developer setup, engineering standards, and PR process.
- **[Code of Conduct](CODE_OF_CONDUCT.md):** Community engagement standards.
