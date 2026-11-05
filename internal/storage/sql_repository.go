package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// dialect captures the few SQL differences between SQLite and PostgreSQL. Queries are written
// with "?" placeholders and rebound for the target database.
type dialect struct {
	name        string
	dollarBinds bool // PostgreSQL uses $1, $2, ...
	noLimit     any  // LIMIT value meaning "no limit": -1 (SQLite) or NULL (PostgreSQL)
}

var (
	sqliteDialect   = dialect{name: "sqlite", noLimit: -1}
	postgresDialect = dialect{name: "postgres", dollarBinds: true, noLimit: nil}
)

// rows per multi-row INSERT; keeps the parameter count far below both databases' limits.
const insertChunk = 250

// SQLRepository implements Repository on database/sql for SQLite and PostgreSQL.
//
// Job documents are stored as JSON next to the indexed columns used for listing and
// coordination (status, timestamps, owner, heartbeat, cancel flag). Record and error writes are
// batched by callers and executed as multi-row INSERTs in one transaction.
type SQLRepository struct {
	db *sql.DB
	d  dialect
}

var _ Repository = (*SQLRepository)(nil)

// Dialect returns "sqlite" or "postgres".
func (r *SQLRepository) Dialect() string { return r.d.name }

// bind converts "?" placeholders to the dialect's syntax. Queries never contain literal "?".
func (r *SQLRepository) bind(q string) string {
	if !r.d.dollarBinds {
		return q
	}
	var sb strings.Builder
	n := 0
	for _, c := range q {
		if c == '?' {
			n++
			sb.WriteString("$" + strconv.Itoa(n))
			continue
		}
		sb.WriteRune(c)
	}
	return sb.String()
}

func (r *SQLRepository) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return r.db.ExecContext(ctx, r.bind(q), args...) //nolint:gosec // query text is a package constant; values are bound parameters
}

func (r *SQLRepository) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return r.db.QueryContext(ctx, r.bind(q), args...) //nolint:gosec // query text is a package constant; values are bound parameters
}

func (r *SQLRepository) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return r.db.QueryRowContext(ctx, r.bind(q), args...) //nolint:gosec // query text is a package constant; values are bound parameters
}

func (r *SQLRepository) limit(n int) any {
	if n <= 0 {
		return r.d.noLimit
	}
	return n
}

// ---------------------------------------------------------------------------------------------
// Jobs

// SaveJob upserts the job document. The cancel flag is never overwritten here, so a cancellation
// requested by another replica survives the owner's progress snapshots.
func (r *SQLRepository) SaveJob(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	_, err = r.exec(ctx, `
INSERT INTO jobs (id, status, created_at, updated_at, owner, heartbeat_at, data) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at,
    owner = excluded.owner, heartbeat_at = excluded.heartbeat_at, data = excluded.data`,
		job.ID, string(job.Status), job.CreatedAt.UnixNano(), job.UpdatedAt.UnixNano(), job.Owner, job.UpdatedAt.UnixNano(), string(data))
	if err != nil {
		return fmt.Errorf("save job %s: %w", job.ID, err)
	}
	return nil
}

// GetJob loads a job by ID.
func (r *SQLRepository) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	var data string
	err := r.queryRow(ctx, `SELECT data FROM jobs WHERE id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get job %s: %w", id, err)
	}
	job, err := decodeJob(data)
	if err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func decodeJob(data string) (*models.Job, error) {
	var job models.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("decode job: %w", err)
	}
	return &job, nil
}

// ListJobs returns matching jobs, newest first, with the total match count.
func (r *SQLRepository) ListJobs(ctx context.Context, opts ListOptions) ([]models.Job, int, error) {
	where, args := "", []any{}
	if opts.Status != "" {
		where, args = " WHERE status = ?", append(args, string(opts.Status))
	}

	var total int
	// where is one of two constant fragments; the status value is a bound parameter.
	if err := r.queryRow(ctx, `SELECT COUNT(1) FROM jobs`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	rows, err := r.query(ctx, `SELECT data FROM jobs`+where+` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`,
		append(args, r.limit(opts.Limit), max(opts.Offset, 0))...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]models.Job, 0)
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, 0, err
		}
		job, err := decodeJob(data)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, *job)
	}
	return jobs, total, rows.Err()
}

// DeleteJob removes a job together with its records and errors.
func (r *SQLRepository) DeleteJob(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`DELETE FROM records WHERE job_id = ?`,
		`DELETE FROM record_errors WHERE job_id = ?`,
		`DELETE FROM jobs WHERE id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, r.bind(q), id); err != nil { //nolint:gosec // query text is a package constant; values are bound parameters
			return fmt.Errorf("delete job %s: %w", id, err)
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------------------------
// Records and errors

// AddErrors inserts a batch of record errors in one transaction.
func (r *SQLRepository) AddErrors(ctx context.Context, jobID string, errs []models.RecordError) error {
	rows := make([][]any, 0, len(errs))
	for _, e := range errs {
		var payload any
		if e.Record != nil {
			b, err := json.Marshal(e.Record)
			if err != nil {
				return err
			}
			payload = string(b)
		}
		rows = append(rows, []any{jobID, e.Source, e.Index, e.Stage, e.Message, payload, e.At.UnixNano()})
	}
	return r.insertRows(ctx, "record_errors", []string{"job_id", "source", "record_index", "stage", "message", "payload", "occurred_at"}, rows)
}

// GetErrors returns a page of errors for a job in insertion order, plus the total count.
func (r *SQLRepository) GetErrors(ctx context.Context, jobID string, offset, limit int) ([]models.RecordError, int, error) {
	total, err := r.count(ctx, `SELECT COUNT(1) FROM record_errors WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.query(ctx, `SELECT source, record_index, stage, message, payload, occurred_at
FROM record_errors WHERE job_id = ? ORDER BY id LIMIT ? OFFSET ?`, jobID, r.limit(limit), max(offset, 0))
	if err != nil {
		return nil, 0, fmt.Errorf("get errors: %w", err)
	}
	defer rows.Close()

	out := make([]models.RecordError, 0)
	for rows.Next() {
		var (
			e       models.RecordError
			payload sql.NullString
			at      int64
		)
		if err := rows.Scan(&e.Source, &e.Index, &e.Stage, &e.Message, &payload, &at); err != nil {
			return nil, 0, err
		}
		e.At = time.Unix(0, at).UTC()
		if payload.Valid {
			_ = json.Unmarshal([]byte(payload.String), &e.Record)
		}
		out = append(out, e)
	}
	return out, total, rows.Err()
}

// SaveRecords inserts a batch of processed records in one transaction.
func (r *SQLRepository) SaveRecords(ctx context.Context, jobID string, records []models.Record) error {
	rows := make([][]any, 0, len(records))
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		rows = append(rows, []any{jobID, string(b)})
	}
	return r.insertRows(ctx, "records", []string{"job_id", "data"}, rows)
}

// GetRecords returns a page of processed records in insertion order, plus the total count.
func (r *SQLRepository) GetRecords(ctx context.Context, jobID string, offset, limit int) ([]models.Record, int, error) {
	total, err := r.count(ctx, `SELECT COUNT(1) FROM records WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.query(ctx, `SELECT data FROM records WHERE job_id = ? ORDER BY id LIMIT ? OFFSET ?`,
		jobID, r.limit(limit), max(offset, 0))
	if err != nil {
		return nil, 0, fmt.Errorf("get records: %w", err)
	}
	defer rows.Close()

	out := make([]models.Record, 0)
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, 0, err
		}
		var rec models.Record
		if err := json.Unmarshal([]byte(data), &rec); err != nil {
			return nil, 0, fmt.Errorf("decode record: %w", err)
		}
		out = append(out, rec)
	}
	return out, total, rows.Err()
}

// insertRows writes rows with multi-row INSERT statements inside one transaction. table and
// columns are compile-time constants from this package, never user input.
func (r *SQLRepository) insertRows(ctx context.Context, table string, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	tuple := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", ") + ")"
	head := "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ") VALUES "
	for start := 0; start < len(rows); start += insertChunk {
		end := min(start+insertChunk, len(rows))
		chunk := rows[start:end]
		args := make([]any, 0, len(chunk)*len(columns))
		tuples := make([]string, len(chunk))
		for i, row := range chunk {
			tuples[i] = tuple
			args = append(args, row...)
		}
		if _, err := tx.ExecContext(ctx, r.bind(head+strings.Join(tuples, ", ")), args...); err != nil { //nolint:gosec // table/columns are package constants; values are bound
			return fmt.Errorf("insert into %s: %w", table, err)
		}
	}
	return tx.Commit()
}

func (r *SQLRepository) count(ctx context.Context, q string, args ...any) (int, error) {
	var n int
	if err := r.queryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------------------------
// Multi-replica coordination

const activeStatuses = `('PENDING', 'RUNNING')`

// Heartbeat records that owner is still running the given jobs.
func (r *SQLRepository) Heartbeat(ctx context.Context, owner string, jobIDs []string, at time.Time) error {
	if len(jobIDs) == 0 {
		return nil
	}
	args := []any{at.UnixNano(), owner}
	args = append(args, anySlice(jobIDs)...)
	_, err := r.exec(ctx, `UPDATE jobs SET heartbeat_at = ? WHERE owner = ? AND id IN (`+placeholders(len(jobIDs))+`)`, args...)
	return err
}

// RequestCancel flags an active job for cancellation.
func (r *SQLRepository) RequestCancel(ctx context.Context, id string) (bool, bool, error) {
	res, err := r.exec(ctx, `UPDATE jobs SET cancel_requested = 1 WHERE id = ? AND status IN `+activeStatuses, id)
	if err != nil {
		return false, false, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, true, nil
	}
	var exists int
	if err := r.queryRow(ctx, `SELECT COUNT(1) FROM jobs WHERE id = ?`, id).Scan(&exists); err != nil {
		return false, false, err
	}
	return exists > 0, false, nil
}

// CancelRequested returns the jobs among jobIDs that have a cancellation request.
func (r *SQLRepository) CancelRequested(ctx context.Context, jobIDs []string) ([]string, error) {
	if len(jobIDs) == 0 {
		return nil, nil
	}
	rows, err := r.query(ctx, `SELECT id FROM jobs WHERE cancel_requested = 1 AND id IN (`+placeholders(len(jobIDs))+`)`, anySlice(jobIDs)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// FailStaleJobs fails active jobs whose heartbeat is older than cutoff. The UPDATE re-checks the
// heartbeat, so a job whose owner heartbeats between the SELECT and the UPDATE is not touched.
func (r *SQLRepository) FailStaleJobs(ctx context.Context, cutoff time.Time, reason string) ([]string, error) {
	stale, err := r.selectJobs(ctx, `SELECT data FROM jobs WHERE status IN `+activeStatuses+` AND heartbeat_at < ?`, cutoff.UnixNano())
	if err != nil {
		return nil, err
	}

	var failed []string
	now := time.Now().UTC()
	for _, job := range stale {
		failStale(job, reason, now)
		data, err := json.Marshal(job)
		if err != nil {
			return failed, err
		}
		res, err := r.exec(ctx, `UPDATE jobs SET status = ?, updated_at = ?, data = ?
WHERE id = ? AND status IN `+activeStatuses+` AND heartbeat_at < ?`,
			string(job.Status), now.UnixNano(), string(data), job.ID, cutoff.UnixNano())
		if err != nil {
			return failed, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			failed = append(failed, job.ID)
		}
	}
	return failed, nil
}

// selectJobs runs a query returning job documents and decodes them.
func (r *SQLRepository) selectJobs(ctx context.Context, q string, args ...any) ([]*models.Job, error) {
	rows, err := r.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []*models.Job
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		job, err := decodeJob(data)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// failStale sets the terminal FAILED state for a job abandoned by its owner.
func failStale(job *models.Job, reason string, now time.Time) {
	job.Status = models.StatusFailed
	job.Error = reason
	job.UpdatedAt = now
	if job.Metrics.EndedAt == nil {
		job.Metrics.EndedAt = &now
	}
	job.Spec = job.Spec.WithRedactedHeaders()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func anySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// ---------------------------------------------------------------------------------------------
// API keys

// CreateAPIKey stores a new key and the SHA-256 hash of its secret.
func (r *SQLRepository) CreateAPIKey(ctx context.Context, k models.APIKey, secretHash string) error {
	_, err := r.exec(ctx, `INSERT INTO api_keys (id, name, prefix, key_hash, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)`,
		k.ID, k.Name, k.Prefix, secretHash, k.CreatedAt.UnixNano(), k.ExpiresAt.UnixNano())
	if err != nil {
		return fmt.Errorf("create api key: %w", err)
	}
	return nil
}

const apiKeyColumns = `id, name, prefix, created_at, expires_at, revoked_at, last_used_at` //nolint:gosec // column list, not a credential

func scanAPIKey(scan func(...any) error) (*models.APIKey, error) {
	var (
		k                 models.APIKey
		created, expires  int64
		revoked, lastUsed sql.NullInt64
	)
	if err := scan(&k.ID, &k.Name, &k.Prefix, &created, &expires, &revoked, &lastUsed); err != nil {
		return nil, err
	}
	k.CreatedAt = time.Unix(0, created).UTC()
	k.ExpiresAt = time.Unix(0, expires).UTC()
	if revoked.Valid {
		t := time.Unix(0, revoked.Int64).UTC()
		k.RevokedAt = &t
	}
	if lastUsed.Valid {
		t := time.Unix(0, lastUsed.Int64).UTC()
		k.LastUsedAt = &t
	}
	return &k, nil
}

// FindAPIKeyByHash looks a key up by the hash of its secret.
func (r *SQLRepository) FindAPIKeyByHash(ctx context.Context, secretHash string) (*models.APIKey, error) {
	k, err := scanAPIKey(r.queryRow(ctx, `SELECT `+apiKeyColumns+` FROM api_keys WHERE key_hash = ?`, secretHash).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return k, err
}

// ListAPIKeys returns every key, newest first.
func (r *SQLRepository) ListAPIKeys(ctx context.Context) ([]models.APIKey, error) {
	rows, err := r.query(ctx, `SELECT `+apiKeyColumns+` FROM api_keys ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.APIKey, 0)
	for rows.Next() {
		k, err := scanAPIKey(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

// RevokeAPIKey revokes a key; revoking an already revoked key keeps the original timestamp.
func (r *SQLRepository) RevokeAPIKey(ctx context.Context, id string, at time.Time) (bool, error) {
	if _, err := r.exec(ctx, `UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL`, at.UnixNano(), id); err != nil {
		return false, err
	}
	n, err := r.count(ctx, `SELECT COUNT(1) FROM api_keys WHERE id = ?`, id)
	return n > 0, err
}

// TouchAPIKey records when a key was last used.
func (r *SQLRepository) TouchAPIKey(ctx context.Context, id string, at time.Time) error {
	_, err := r.exec(ctx, `UPDATE api_keys SET last_used_at = ? WHERE id = ?`, at.UnixNano(), id)
	return err
}

// DeleteAPIKeysBefore purges keys that expired or were revoked before cutoff.
func (r *SQLRepository) DeleteAPIKeysBefore(ctx context.Context, cutoff time.Time) (int, error) {
	c := cutoff.UnixNano()
	res, err := r.exec(ctx, `DELETE FROM api_keys WHERE expires_at < ? OR (revoked_at IS NOT NULL AND revoked_at < ?)`, c, c)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---------------------------------------------------------------------------------------------

// Ping verifies the database is reachable (used by the readiness probe).
func (r *SQLRepository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// Close releases the database handle.
func (r *SQLRepository) Close() error {
	return r.db.Close()
}

// ---------------------------------------------------------------------------------------------
// Migrations

type migration struct {
	version    int
	name       string
	statements []string
}

// txBeginner is satisfied by *sql.DB and *sql.Conn (PostgreSQL migrates on a locked connection).
type txBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// migrate applies every migration not yet recorded in schema_migrations, each in its own transaction.
func (r *SQLRepository) migrate(ctx context.Context, conn txBeginner, migrations []migration) error {
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for _, m := range migrations {
		if err := r.applyMigration(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

func (r *SQLRepository) applyMigration(ctx context.Context, conn txBeginner, m migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback() }()

	var applied int
	if err := tx.QueryRowContext(ctx, r.bind(`SELECT COUNT(1) FROM schema_migrations WHERE version = ?`), m.version).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %d: %w", m.version, err)
	}
	if applied > 0 {
		return nil
	}
	for _, stmt := range m.statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migration %d (%s) failed: %w", m.version, m.name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, r.bind(`INSERT INTO schema_migrations (version, name) VALUES (?, ?)`), m.version, m.name); err != nil {
		return fmt.Errorf("record migration %d: %w", m.version, err)
	}
	return tx.Commit()
}
