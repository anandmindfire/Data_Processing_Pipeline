package api

import (
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/api/middleware"
	"github.com/anandkr47/data-processing-pipeline/internal/config"
	"github.com/anandkr47/data-processing-pipeline/internal/export"
	"github.com/anandkr47/data-processing-pipeline/internal/ingestion"
	"github.com/anandkr47/data-processing-pipeline/internal/jobs"
	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
	"github.com/anandkr47/data-processing-pipeline/internal/transformation"
	"github.com/anandkr47/data-processing-pipeline/internal/validation"
)

// JobCoordinator defines the behavior expected from a job manager.
type JobCoordinator interface {
	SubmitJob(r *http.Request, cfg interface{})
}

// Server provides the HTTP REST interface for pipeline management and metrics.
type Server struct {
	config      *config.Config
	manager     *jobs.Manager
	rateLimiter *middleware.RateLimiter
	handler     http.Handler
}

// New constructs a Server using a storage.Store adapter (backward-compatible).
func New(store *storage.Store) *Server {
	cfg := config.Load()
	repo := store.Repository()

	pipe := pipeline.NewPipeline(
		ingestion.NewReader(http.DefaultClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		repo,
		metrics.GlobalCollector,
	)
	mgr := jobs.NewManager(cfg, repo, pipe)
	return NewServer(cfg, mgr)
}

// NewServer constructs a Server with the given configuration and job manager.
func NewServer(cfg *config.Config, mgr *jobs.Manager) *Server {
	if cfg == nil {
		cfg = config.Load()
	}
	s := &Server{
		config:      cfg,
		manager:     mgr,
		rateLimiter: middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst),
	}
	s.setupRoutes()
	return s
}

// ServeHTTP delegates to the configured middleware and router chain.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
