# Design Report — Concurrent Data Processing Pipeline

## 1. Executive Summary & Architecture Overview
This service implements an asynchronous, concurrent data processing engine in Go. A REST API ingests job specifications defining heterogeneous input sources (CSV, JSON, HTTP REST APIs), validation rules, transformation pipelines, analytical aggregations, and export destinations. The system adheres to Clean Architecture principles, decoupling transport, domain models, pipeline engine stages, telemetry, and persistence behind clean interfaces (`storage.Repository`, `ingestion.Reader`, `export.Exporter`).

```
[Sources: CSV/JSON/HTTP] ──► recordsCh ──► [Validation Pool] ──► validatedCh ──► [Transform Pool] ──► transformedCh ──► [Aggregation/Collector] ──► [Export Stage]
                                                 │                                      │
                                                 └───────────────────┬──────────────────┘
                                                                     ▼
                                                         [Centralized ErrorCh] ──► [Repository & Dead-Letter Sink]
```

## 2. Concurrency Model & Goroutine Lifecycle
The pipeline is modeled as a bounded directed acyclic channel graph:
1. **Ingestion Fan-Out:** Each input source streams records concurrently into a bounded channel (`recordsCh`), providing backpressure on disk I/O and network requests.
2. **Worker Pool Fan-Out:** Parameterized worker pools parallelize CPU-bound validation and transformation stages. Workers pull from input channels and emit downstream.
3. **Channel Ownership:** Upstream coordinators own channels and close them strictly after their associated `sync.WaitGroup` completes, ensuring workers never send on closed channels.
4. **Fan-In Aggregation Collector:** Rather than using mutex locks across worker goroutines, a single consumer drains `transformedCh`. Single ownership eliminates map concurrency races and lock contention on the hot path.
5. **Centralized Error Isolation:** Corrupted rows are quarantined with stage metadata and sent over `errorCh` to an asynchronous persistence collector. Faulty records never crash the pipeline.

## 3. Cooperative Cancellation & Context Hierarchy
Every pipeline job executes under a hierarchical `context.Context` derived from `context.WithTimeout` and `context.WithCancel`. Cancellation propagates to file readers, HTTP requests (`http.NewRequestWithContext`), and worker `select` statements. When cancelled via `PATCH /api/v1/pipelines/:id/cancel` or job timeout, worker loops terminate cleanly, resources are released, and terminal state (`CANCELLED` or `FAILED`) is saved using a dedicated non-cancelled timeout context.

## 4. Design Choices & Trade-Offs

| Decision | Choice Made | Rationale & Trade-offs |
| :--- | :--- | :--- |
| **Persistence** | One `database/sql` repository with SQLite (default) and PostgreSQL dialects | SQLite needs no external service for a single node; PostgreSQL enables replicas. Shared code means one contract test suite runs against memory, SQLite and PostgreSQL. |
| **Multi-replica coordination** | Job owner + heartbeat lease, cancel flag polled by the owner, stale-job recovery | No message broker needed; cancel latency ~1 s, crash detection ≈ `JOB_LEASE`. A partitioned owner could finish a job after it was failed (its final save wins). |
| **API keys** | Admin key mints expiring, revocable keys, stored as SHA-256 hashes, cached 15 s | Full-entropy secrets make fast hashing safe; the cache keeps auth off the database on hot paths at the cost of ≤ 15 s revocation lag across replicas. |
| **Live Progress** | Progress tracker owns `job.Metrics` and persists a snapshot every 500 ms | Progress is visible while the job runs without sharing mutable state across goroutines; the trade-off is up to 500 ms of staleness. `percent_complete` is relative to records discovered so far, because streamed sources have no known total. |
| **Aggregation** | Single fan-in aggregator by default; optional `aggregation_workers` partials merged at the end | Lock-free either way. Partials pay off when many CPU-heavy definitions are computed; the merge is exact because count/sum/avg/min/max are associative. Memory scales with group cardinality × workers. |
| **Fault Tolerance** | Quarantine pattern + Dead-Letter sink | Isolates corrupted records with stage metadata; 1 bad row does not fail the batch. Unrecoverable rows exported to disk. |
| **Ingestion Resilience** | HTTP 3x exponential backoff retry | Transparently absorbs transient network hiccups and 5xx server errors without failing the pipeline. |
| **Streaming Export** | Export goroutine streams batches to JSON, CSV, a real SQLite database and a SQL dump; files are published by atomic rename | Bounded memory (CSV/SQL headers are spooled to disk) and no partial files for cancelled/failed jobs. Aggregations go to an `aggregations` table or a JSON sidecar. |
| **Security Boundaries** | Input paths sandboxed to `INPUT_DIRS`, outputs to `EXPORT_DIR`; dial-time SSRF checks; per-job resource limits | The API accepts file paths and URLs from clients, so the server, not the client, decides what it may read, write, or connect to. |
| **Lifecycle** | `WithCancelCause` per job; graceful drain on `SIGTERM`; restart reconciliation | Every terminal job records why it ended; no job is left `RUNNING` after a crash. |
| **API Transport & Routing** | `net/http` + `chi/v5` router | Zero-allocation routing with subrouter mounting, URL parameter extraction, and standard middleware chaining. |

See [production-readiness.md](production-readiness.md) and [SECURITY.md](../SECURITY.md) for the roadmap bridging this codebase into multi-node Kubernetes enterprise deployments.
