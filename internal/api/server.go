package api

import (
	"net/http"

	"github.com/anandkr47/data-processing-pipeline/internal/api/middleware"
	"github.com/anandkr47/data-processing-pipeline/internal/config"
	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
	"github.com/anandkr47/data-processing-pipeline/internal/repository"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
)

// Server provides the HTTP REST interface for pipeline management and metrics.
type Server struct {
	config      *config.Config
	manager     *pipeline.Manager
	rateLimiter *middleware.RateLimiter
	handler     http.Handler
}

// New creates an HTTP Server using a storage.Store adapter (backward-compatible).
func New(store *storage.Store) *Server {
	cfg := config.Load()
	repo := store.Repository()
	executor := pipeline.NewExecutor(repo, http.DefaultClient, cfg.ExportDir, metrics.GlobalCollector)
	mgr := pipeline.NewManager(cfg, repo, executor)
	return NewServer(cfg, mgr)
}

// NewServer constructs a Server with the given configuration and pipeline manager.
func NewServer(cfg *config.Config, mgr *pipeline.Manager) *Server {
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

// NewServerWithRepository creates a Server backed by any Repository implementation.
func NewServerWithRepository(cfg *config.Config, repo repository.Repository) *Server {
	if cfg == nil {
		cfg = config.Load()
	}
	executor := pipeline.NewExecutor(repo, http.DefaultClient, cfg.ExportDir, metrics.GlobalCollector)
	mgr := pipeline.NewManager(cfg, repo, executor)
	return NewServer(cfg, mgr)
}

// ServeHTTP delegates to the configured middleware and router chain.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
