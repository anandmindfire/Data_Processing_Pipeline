package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
	_ "modernc.org/sqlite"
)

// SQLiteDriver is the registered driver name for modernc.org/sqlite.
const SQLiteDriver = "sqlite"

// SQLiteDBFile is the database filename created inside the data directory.
const SQLiteDBFile = "pipeline.db"

// SQLiteRepository persists jobs, processed records, and record errors in an embedded SQLite database.
// Writes are batched by callers (SaveRecords/AddErrors) and executed in a single transaction each.
type SQLiteRepository struct {
	db *sql.DB
}

var _ Repository = (*SQLiteRepository)(nil)

// NewSQLiteRepository opens (or creates) <dataDir>/pipeline.db and applies schema migrations.
func NewSQLiteRepository(ctx context.Context, dataDir string) (*SQLiteRepository, error) {
	db, err := openSQLite(ctx, filepath.Join(dataDir, SQLiteDBFile), 4)
	if err != nil {
		return nil, err
	}
	if err := migrateSQLite(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteRepository{db: db}, nil
}

// SaveJob upserts the full job document.
func (r *SQLiteRepository) SaveJob(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO jobs (id, status, created_at, updated_at, data) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at, data = excluded.data`,
		job.ID, string(job.Status), job.CreatedAt.UnixNano(), job.UpdatedAt.UnixNano(), string(data))
	if err != nil {
		return fmt.Errorf("save job %s: %w", job.ID, err)
	}
	return nil
}

// GetJob loads a job by ID.
func (r *SQLiteRepository) GetJob(ctx context.Context, id string) (*models.Job, bool, error) {
	var data string
	err := r.db.QueryRowContext(ctx, `SELECT data FROM jobs WHERE id = ?`, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get job %s: %w", id, err)
	}
	var job models.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, false, fmt.Errorf("decode job %s: %w", id, err)
	}
	return &job, true, nil
}

// ListJobs returns matching jobs, newest first, with the total match count.
func (r *SQLiteRepository) ListJobs(ctx context.Context, opts ListOptions) ([]models.Job, int, error) {
	where, args := "", []any{}
	if opts.Status != "" {
		where, args = " WHERE status = ?", append(args, string(opts.Status))
	}

	var total int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = -1 // SQLite: no limit
	}
	rows, err := r.db.QueryContext(ctx, `SELECT data FROM jobs`+where+` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`,
		append(args, limit, max(opts.Offset, 0))...)
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
		var job models.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			return nil, 0, fmt.Errorf("decode job: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, total, rows.Err()
}

// DeleteJob removes a job together with its records and errors.
func (r *SQLiteRepository) DeleteJob(ctx context.Context, id string) error {
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
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return fmt.Errorf("delete job %s: %w", id, err)
		}
	}
	return tx.Commit()
}

// AddErrors inserts a batch of record errors in one transaction.
func (r *SQLiteRepository) AddErrors(ctx context.Context, jobID string, errs []models.RecordError) error {
	if len(errs) == 0 {
		return nil
	}
	return r.inTx(ctx, `INSERT INTO record_errors (job_id, source, record_index, stage, message, payload, occurred_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`, func(stmt *sql.Stmt) error {
		for _, e := range errs {
			var payload any
			if e.Record != nil {
				b, err := json.Marshal(e.Record)
				if err != nil {
					return err
				}
				payload = string(b)
			}
			if _, err := stmt.ExecContext(ctx, jobID, e.Source, e.Index, e.Stage, e.Message, payload, e.At.UnixNano()); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetErrors returns a page of errors for a job in insertion order, plus the total count.
func (r *SQLiteRepository) GetErrors(ctx context.Context, jobID string, offset, limit int) ([]models.RecordError, int, error) {
	total, err := r.count(ctx, `SELECT COUNT(1) FROM record_errors WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT source, record_index, stage, message, payload, occurred_at
FROM record_errors WHERE job_id = ? ORDER BY id LIMIT ? OFFSET ?`, jobID, sqlLimit(limit), max(offset, 0))
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
func (r *SQLiteRepository) SaveRecords(ctx context.Context, jobID string, records []models.Record) error {
	if len(records) == 0 {
		return nil
	}
	return r.inTx(ctx, `INSERT INTO records (job_id, data) VALUES (?, ?)`, func(stmt *sql.Stmt) error {
		for _, rec := range records {
			b, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if _, err := stmt.ExecContext(ctx, jobID, string(b)); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetRecords returns a page of processed records in insertion order, plus the total count.
func (r *SQLiteRepository) GetRecords(ctx context.Context, jobID string, offset, limit int) ([]models.Record, int, error) {
	total, err := r.count(ctx, `SELECT COUNT(1) FROM records WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT data FROM records WHERE job_id = ? ORDER BY id LIMIT ? OFFSET ?`,
		jobID, sqlLimit(limit), max(offset, 0))
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

// Close releases the database handle.
func (r *SQLiteRepository) Close() error {
	return r.db.Close()
}

func (r *SQLiteRepository) inTx(ctx context.Context, query string, fn func(*sql.Stmt) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()
	if err := fn(stmt); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *SQLiteRepository) count(ctx context.Context, query string, args ...any) (int, error) {
	var n int
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return n, nil
}

func sqlLimit(limit int) int {
	if limit <= 0 {
		return -1
	}
	return limit
}

func sqliteDSN(filePath string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	return "file:" + filePath + "?" + q.Encode()
}

func openSQLite(ctx context.Context, filePath string, maxOpenConns int) (*sql.DB, error) {
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create sqlite directory: %w", err)
		}
	}
	db, err := sql.Open(SQLiteDriver, sqliteDSN(filePath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if maxOpenConns > 0 {
		db.SetMaxOpenConns(maxOpenConns)
		db.SetMaxIdleConns(maxOpenConns)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}
	return db, nil
}

type migration struct {
	version int
	name    string
	sql     string
}

var storeMigrations = []migration{
	{
		version: 1,
		name:    "create_jobs_records_errors",
		sql: `
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    status     TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    data       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

CREATE TABLE IF NOT EXISTS records (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    data   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_job ON records(job_id, id);

CREATE TABLE IF NOT EXISTS record_errors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    source       TEXT    NOT NULL,
    record_index INTEGER NOT NULL,
    stage        TEXT    NOT NULL,
    message      TEXT    NOT NULL,
    payload      TEXT,
    occurred_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_record_errors_job ON record_errors(job_id, id);
`,
	},
}

func migrateSQLite(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	for _, m := range storeMigrations {
		if err := applyOneMigration(ctx, db, m); err != nil {
			return err
		}
	}
	return nil
}

func applyOneMigration(ctx context.Context, db *sql.DB, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback() }()

	var applied int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM schema_migrations WHERE version = ?`, m.version).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %d: %w", m.version, err)
	}
	if applied > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return fmt.Errorf("migration %d (%s) failed: %w", m.version, m.name, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name) VALUES (?, ?)`, m.version, m.name); err != nil {
		return fmt.Errorf("record migration %d: %w", m.version, err)
	}
	return tx.Commit()
}
