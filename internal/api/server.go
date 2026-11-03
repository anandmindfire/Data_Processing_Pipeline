package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Server struct {
	store   *storage.Store
	engine  *pipeline.Engine
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
	mux     *http.ServeMux
}

func New(store *storage.Store) *Server {
	s := &Server{store: store, engine: &pipeline.Engine{Store: store, Client: &http.Client{Timeout: 30 * time.Second}}, cancels: map[string]context.CancelFunc{}, mux: http.NewServeMux()}
	s.routes()
	return s
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }
func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	s.mux.HandleFunc("/metrics", s.metrics)
	s.mux.HandleFunc("/api/v1/pipelines", s.collection)
	s.mux.HandleFunc("/api/v1/pipelines/", s.item)
}
func (s *Server) collection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var c domain.Config
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&c); err != nil {
			writeErr(w, 400, "invalid JSON: "+err.Error())
			return
		}
		if len(c.Sources) == 0 {
			writeErr(w, 400, "at least one source is required")
			return
		}
		if len(c.Exports) == 0 {
			writeErr(w, 400, "at least one export target is required")
			return
		}
		now := time.Now()
		j := &domain.Job{ID: newID(), Status: domain.Pending, Config: c, CreatedAt: now, UpdatedAt: now, Metrics: domain.Metrics{StageLatency: map[string]time.Duration{}}}
		if err := s.store.PutJob(j); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		s.cancels[j.ID] = cancel
		s.mu.Unlock()
		responseJob, _ := s.store.GetJob(j.ID)
		go func() {
			defer func() { s.mu.Lock(); delete(s.cancels, j.ID); s.mu.Unlock() }()
			s.engine.Run(ctx, j, func(updated *domain.Job) { _ = s.store.PutJob(updated) })
		}()
		write(w, 202, responseJob)
	case http.MethodGet:
		write(w, 200, s.store.ListJobs())
	default:
		w.Header().Set("Allow", "GET, POST")
		writeErr(w, 405, "method not allowed")
	}
}
func (s *Server) item(w http.ResponseWriter, r *http.Request) {
	p := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/"), "/")
	parts := strings.Split(p, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, 404, "not found")
		return
	}
	id := parts[0]
	j, ok := s.store.GetJob(id)
	if !ok {
		writeErr(w, 404, "pipeline not found")
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			write(w, 200, j)
		case http.MethodDelete:
			s.mu.Lock()
			_, running := s.cancels[id]
			s.mu.Unlock()
			if running {
				writeErr(w, 409, "cancel the running job before deleting it")
				return
			}
			for _, target := range j.Config.Exports {
				path := target.Path
				if path == "" && target.Type != "" {
					path = filepath.Join("exports", j.ID+"."+target.Type)
				}
				if path != "" {
					if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
						writeErr(w, 500, "remove export artifact: "+err.Error())
						return
					}
				}
			}
			if err := s.store.DeleteJob(id); err != nil {
				writeErr(w, 500, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			writeErr(w, 405, "method not allowed")
		}
		return
	}
	if len(parts) != 2 {
		writeErr(w, 404, "not found")
		return
	}
	switch parts[1] {
	case "progress":
		if r.Method != http.MethodGet {
			writeErr(w, 405, "method not allowed")
			return
		}
		m := j.Metrics
		percent := 0.0
		if m.Discovered > 0 {
			percent = float64(m.Processed+m.Rejected) / float64(m.Discovered) * 100
		}
		if j.Status == domain.Completed || j.Status == domain.Failed || j.Status == domain.Cancelled {
			percent = 100
		}
		pending := m.Discovered - m.Processed - m.Rejected
		if pending < 0 {
			pending = 0
		}
		write(w, 200, domain.Progress{JobID: id, Status: j.Status, Percent: percent, Processed: m.Processed, Discovered: m.Discovered, Pending: pending, Valid: m.Valid, Rejected: m.Rejected, ErrorCount: m.Errors, StartedAt: m.StartedAt, EndedAt: m.EndedAt, RecordsPerSecond: m.RecordsPerSecond, StageLatency: m.StageLatency})
	case "results":
		if r.Method != http.MethodGet {
			writeErr(w, 405, "method not allowed")
			return
		}
		write(w, 200, map[string]any{"job_id": id, "records": s.store.Records(id), "aggregations": j.Aggregates, "export_path": j.ResultsPath})
	case "errors":
		if r.Method != http.MethodGet {
			writeErr(w, 405, "method not allowed")
			return
		}
		write(w, 200, map[string]any{"job_id": id, "errors": s.store.Errors(id)})
	case "cancel":
		if r.Method != http.MethodPatch {
			writeErr(w, 405, "method not allowed")
			return
		}
		s.mu.Lock()
		cancel, exists := s.cancels[id]
		s.mu.Unlock()
		if !exists {
			writeErr(w, 409, "pipeline is not running")
			return
		}
		cancel()
		write(w, 202, map[string]string{"status": "cancellation_requested", "job_id": id})
	default:
		writeErr(w, 404, "not found")
	}
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, 405, "method not allowed")
		return
	}
	jobs := s.store.ListJobs()
	var total, processed, errs int64
	for _, j := range jobs {
		total++
		processed += j.Metrics.Processed
		errs += j.Metrics.Errors
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte("pipeline_jobs_total " + itoa(total) + "\npipeline_records_processed_total " + itoa(processed) + "\npipeline_errors_total " + itoa(errs) + "\n"))
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, status int, msg string) {
	write(w, status, map[string]string{"error": msg})
}
func newID() string { return time.Now().UTC().Format("20060102T150405.000000000") }
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	b := make([]byte, 0, 20)
	for v > 0 {
		b = append(b, byte('0'+v%10))
		v /= 10
	}
	if neg {
		b = append(b, '-')
	}
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return string(b)
}

var _ = errors.Is
