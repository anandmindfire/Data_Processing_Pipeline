# Design Report — Concurrent Data Processing Pipeline

## 1. Executive Summary & Architecture Overview
This service implements an asynchronous, concurrent data processing engine in Go. A REST API ingests job specifications specifying heterogeneous input sources (CSV, JSON, HTTP REST APIs), validation rules, transformation pipelines, analytical aggregations, and export destinations. The application follows clean architectural principles, decoupling the HTTP transport, domain models, pipeline engine stages, telemetry, and persistence behind clean interfaces (`repository.Repository`).

```
[Sources: CSV/JSON/HTTP] ──► rawCh ──► [Validation Pool] ──► validCh ──► [Transform Pool] ──► transformedCh ──► [Aggregation/Collector] ──► [Export Stage]
                                            │                                  │
                                            └────────────────┬─────────────────┘
                                                             ▼
                                                    [Centralized ErrorCh] ──► [Repository & Metrics]
```

## 2. Concurrency Model & Goroutine Lifecycle
The pipeline is modeled as a bounded directed acyclic channel graph:
1. **Ingestion Fan-Out:** Each input source executes in its own dedicated goroutine, streaming parsed records (`sources.Item`) into a bounded channel (`rawCh`). Bounded channels impose backpressure on disk I/O and network calls, preventing out-of-memory errors.
2. **Worker Pool Fan-Out:** Configurable worker pools parallelize CPU-bound validation and transformation stages. Workers pull from input channels and emit to downstream stages.
3. **Channel Ownership:** Upstream coordinators own channels and close them strictly after their associated `sync.WaitGroup` completes. This guarantees that workers never send on closed channels.
4. **Fan-In Aggregation Collector:** Rather than using complex lock synchronization across worker goroutines, a single collector drains `transformedCh`. Single ownership eliminates map concurrency races and lock contention on the hot path.
5. **Centralized Error Isolation:** Corrupted rows are quarantined with stage metadata and sent over `errCh` to an asynchronous persistence collector. Faulty records never crash the pipeline.

## 3. Cooperative Cancellation & Context Hierarchy
Every pipeline job executes under a hierarchical `context.Context` derived from `context.WithTimeout` and `context.WithCancel`. Cancellation propagates to file readers, HTTP requests (`http.NewRequestWithContext`), and worker `select` statements. When cancelled via `PATCH /api/v1/pipelines/:id/cancel` or job timeout, worker loops terminate cleanly, resources are released, and terminal state (`CANCELLED` or `FAILED`) is saved.

## 4. Design Choices & Trade-Offs

| Decision | Choice Made | Rationale & Trade-offs |
| :--- | :--- | :--- |
| **Persistence** | `repository.Repository` interface with `FileRepository` & `MemoryRepository` | Adheres to assignment requirements without external database overhead while providing atomic crash-safety via temporary file write and rename. Ready for drop-in PostgreSQL migration. |
| **State Synchronization** | Fan-In sequential aggregation | Avoids mutex contention in the critical data path. Memory footprint scales with aggregate group cardinality. |
| **Error Handling** | Stage error quarantine pattern | Real-world resilience: 1 invalid row out of 1,000,000 must not abort the remaining 999,999 valid records. |
| **Security & Reliability** | In-memory token bucket & concurrency quotas | Protects API from denial-of-service and memory exhaustion without external Redis dependencies. |
| **Dependencies** | 100% Go Standard Library | Zero supply-chain CVE vulnerabilities, instant builds, and maximum cross-platform portability. |

See [docs/production-readiness.md](file:///Users/anandk/Projects/data-processing-pipeline/docs/production-readiness.md) for the roadmap bridging this codebase into multi-node Kubernetes enterprise deployments.
