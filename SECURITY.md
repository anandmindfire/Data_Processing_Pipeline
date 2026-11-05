# Security Policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/anandmindfire/Data_Processing_Pipeline/security/advisories/new)
rather than in a public issue. Include the affected version (`pipeline-server --version` or
`GET /healthz`), reproduction steps, and the impact you observed.

## Supported versions

Only the latest commit on `main` receives security fixes.

## Threat model

The service runs jobs described by API clients. A job spec names local files, remote URLs,
output paths, and resource sizes, so a malicious or careless client could otherwise make the
server read secrets, write anywhere, reach internal services, or exhaust its resources. The
controls below keep those decisions on the server.

| Threat | Control | Where |
| :--- | :--- | :--- |
| Long-lived or leaked credentials | Generated keys expire (1 minute to `API_KEY_MAX_TTL`), are revocable, stored only as SHA-256 hashes, shown once, and cannot manage keys; `MAX_API_KEYS` caps active keys; `key_id` is logged per request | `internal/auth`, `middleware/auth.go` |
| Unauthenticated access | API keys (`X-API-Key` or `Bearer`), ≥ 16 chars, several keys for rotation, SHA-256 + constant-time comparison; `/metrics` protected unless `METRICS_PUBLIC=true` | `internal/api/middleware/auth.go` |
| Reading arbitrary files | Local sources must resolve (symlinks included) inside `INPUT_DIRS` | `internal/api/validator/paths.go` |
| Writing / deleting arbitrary files | Exports and `dlq_path` must resolve inside `EXPORT_DIR`; delete and retention only remove files inside it | `validator/paths.go`, `services/export_service.go` |
| SSRF to internal services or cloud metadata | Destination IP checked at dial time on every connection and redirect (private, loopback, link-local, CGNAT, multicast, metadata); environment proxies ignored; at most 5 redirects | `internal/ingestion/ssrf.go` |
| Resource exhaustion | Limits on workers, buffers, batches, aggregation workers, timeout, sources, exports; per-source byte cap (`MAX_SOURCE_BYTES`); request body cap; `MAX_ACTIVE_JOBS`; per-IP token bucket with a bounded client table | `validator`, `ingestion/limit.go`, `middleware/ratelimit.go` |
| Rate-limit bypass via spoofed headers | `X-Forwarded-For` honoured only from `TRUSTED_PROXIES`, walked right to left | `middleware/clientip.go` |
| Malformed or ambiguous requests | Strict JSON (unknown fields and trailing data rejected), `Content-Type` enforced, all spec errors reported together | `handlers/pipeline_handler.go` |
| SQL injection | Values always bound as parameters; identifiers from record keys quoted | `storage/sqlite.go`, `export/sqlite_sink.go` |
| XSS / clickjacking | `Content-Security-Policy: default-src 'none'` and related headers on API responses; Swagger UI pinned with Subresource Integrity and a hash-based CSP | `middleware/security.go`, `handlers/docs_handler.go` |
| Credential leakage via job specs | HTTP source header values are redacted in every API response and dropped from the stored job when it finishes | `models.JobSpec.WithRedactedHeaders` |
| Log injection | Client-supplied `X-Request-ID` accepted only if short and alphanumeric; query strings and credentials are not logged | `middleware/request_id.go`, `middleware/logger.go` |
| Container breakout / privilege | Static binary in distroless, uid 65532, read-only root FS, all capabilities dropped, `no-new-privileges` | `Dockerfile`, `docker-compose.yml` |
| Vulnerable dependencies | Patched Go toolchain pinned in `go.mod`; govulncheck, gosec and Trivy in CI; actions pinned to SHAs; Dependabot | `.github/` |
| Data retention | Finished jobs, records, errors and exported files purged after `JOB_RETENTION` | `services/retention.go` |

## Production deployment checklist

- [ ] Set `API_KEY` to a random value of at least 32 characters (`openssl rand -hex 32`) and keep it in a secret store, not in the image or the repository. Treat it as the admin credential: hand clients generated, expiring keys instead.
- [ ] For more than one replica, set `DATABASE_URL` (with `sslmode=require` or stricter) and mount a shared `EXPORT_DIR`. Give the database user only rights on its own database.
- [ ] Terminate TLS at a reverse proxy or load balancer, and set `TRUSTED_PROXIES` to its addresses.
- [ ] Keep `ALLOW_PRIVATE_SOURCES=false`. If jobs must read internal APIs, allow them through an egress proxy or network policy instead.
- [ ] Keep `CORS_ALLOWED_ORIGINS` empty unless a browser front end needs it, and list exact origins.
- [ ] Scrape `/metrics` with the API key, or set `METRICS_PUBLIC=true` only on a private network.
- [ ] Consider `DOCS_ENABLED=false` on internet-facing deployments.
- [ ] Mount `DATA_DIR` and `EXPORT_DIR` on persistent volumes and back up `pipeline.db`.
- [ ] Size `MAX_ACTIVE_JOBS`, `MAX_SOURCE_BYTES` and container CPU/memory limits together.
- [ ] Set `JOB_RETENTION` to match your data-retention policy.
- [ ] Give the orchestrator a termination grace period longer than `SHUTDOWN_TIMEOUT`.

## Known limitations

- Generated keys share one permission level (all pipeline endpoints); there are no per-key
  scopes yet. Per-request `key_id` logging is available, but there is no separate audit store.
- Revoking a key takes up to 15 seconds to reach other replicas (verification cache).
- HTTP source headers (for example `Authorization` for a third-party API) are never returned by
  the API and are removed from the stored job once it finishes, but they are kept in the job
  store while the job is pending or running. Prefer short-lived tokens in job specs.
