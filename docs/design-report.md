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
| **Persistence** | `storage.Repository` interface (`FileRepository` & `MemoryRepository`) | Adheres to assignment requirements without external DB overhead; atomic crash-safety via temp write + rename. Ready for drop-in PostgreSQL migration. |
| **State Synchronization** | Fan-In sequential aggregation | Eliminates mutex contention on the hot path. Memory footprint scales with aggregate group cardinality. |
| **Fault Tolerance** | Quarantine pattern + Dead-Letter sink | Isolates corrupted records with stage metadata; 1 bad row does not fail the batch. Unrecoverable rows exported to disk. |
| **Ingestion Resilience** | HTTP 3x exponential backoff retry | Transparently absorbs transient network hiccups and 5xx server errors without failing the pipeline. |
| **Multi-Sink Export** | JSON, RFC 4180 CSV & Transactional SQLite dump | Generates schema DDL and bulk INSERT transactions importable into SQLite without CGO build complexity. |
| **API Transport & Routing** | `net/http` + `chi/v5` router | Zero-allocation routing with subrouter mounting, URL parameter extraction, and standard middleware chaining. |

See [docs/production-readiness.md](docs/production-readiness.md) for the roadmap bridging this codebase into multi-node Kubernetes enterprise deployments.
