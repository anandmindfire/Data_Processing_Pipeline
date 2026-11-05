# Design Report — Concurrent Data Processing Pipeline

**What it is.** A Go service that runs data-processing jobs submitted over a REST API. A job spec
names CSV/JSON/HTTP sources, validation and transformation rules, aggregations, export targets,
and worker counts. Jobs run as a multi-stage goroutine pipeline; progress, metrics, errors and
results are queryable while the job runs and after it finishes.

## Concurrency model

```
sources (1 goroutine each) ─► recordsCh ─► validate (N workers) ─► validatedCh ─► transform (N workers)
   ─► transformedCh ─► aggregate (1, or N partials ─► partialCh ─► merger) ─► resultCh ─► export (1)
all stages ─► errorCh ─► error collector (1)          all stages ─► progressCh ─► progress tracker (1)
```

- **Fan-out / fan-in:** one reader goroutine per source; validation and transformation use worker
  pools sized per job; aggregation is a single fan-in goroutine or, with `aggregation_workers`,
  lock-free partial aggregators merged at the end (exact, since count/sum/avg/min/max are associative).
- **Channels:** typed and bounded (`buffer_size`), so a slow stage applies backpressure to the
  stages before it instead of growing memory. Each channel is closed only by its writers' owner,
  after their `sync.WaitGroup` completes, so no goroutine ever sends on a closed channel.
- **Single owners instead of locks:** the aggregator owns its maps, the progress tracker owns the
  job metrics (snapshotting them to storage every 500 ms), and the error collector owns error
  persistence (batched).
- **Context:** each job runs under `WithCancelCause` + `WithTimeout`; readers, HTTP requests and every
  channel send `select` on it, so cancel, timeout or `SIGTERM` stop all workers promptly and the
  reason is recorded. Final state is saved with a context detached from the cancellation.

## Design choices and trade-offs

| Decision | Trade-off |
| :--- | :--- |
| Invalid records are quarantined (error store + optional DLQ file) and processing continues | A job "completes" with rejections; callers must check `rejected` / `/errors` |
| Export streams batches and publishes files by atomic rename; records also go to the job store | No partial files on cancel/failure, at the cost of temp `.part` files and disk spooling for CSV/SQL headers |
| `percent_complete` = handled / discovered so far | Streamed sources have no known total; the figure rises while ingestion is ahead |
| One `database/sql` repository with SQLite (default) and PostgreSQL dialects | SQLite needs no service for one node; PostgreSQL enables replicas. One contract test suite covers both |
| Replicas coordinate through the store (owner heartbeats, cancel flag, lease-based recovery) | No broker needed; cancel latency ≈ 1 s and crash detection ≈ `JOB_LEASE`. Export storage must be shared |
| Server-side sandboxes and limits (input/export dirs, SSRF checks at dial time, size and worker caps) | Clients cannot read, write or reach anything the operator did not allow; specs outside the limits get 400 |
| Admin key mints expiring, revocable, hashed API keys (15 s verification cache) | Keeps auth off the database on the hot path; revocation reaches other replicas within 15 s |
| Rules combine validation and transformation per field | Compact specs; very complex mappings would need a scripting stage |

Operational details (configuration, security controls, scaling) are in the [README](../README.md),
[SECURITY.md](../SECURITY.md) and [production-readiness.md](production-readiness.md).
