package api

import (
	"net/http"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/routes"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
	"github.com/go-chi/chi/v5"
)

// Server provides the HTTP REST interface for pipeline management and metrics.
type Server struct {
	config     *configs.Config
	controller *controllers.PipelineController
	handler    http.Handler
}

// New constructs a Server using a storage.Store adapter (backward-compatible).
func New(store *storage.Store) *Server {
	cfg := configs.Load()
	repo := store.Repository()

	pipe := pipeline.NewPipeline(
		ingestion.NewReader(http.DefaultClient),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		repo,
		metrics.GlobalCollector,
	)
	service := services.NewPipelineService(cfg, repo, pipe)
	ctrl := controllers.NewPipelineController(service, cfg.MaxRequestBodyBytes)
	return NewServer(cfg, ctrl)
}

// NewServer constructs a Server with the given configuration and controller.
func NewServer(cfg *configs.Config, ctrl *controllers.PipelineController) *Server {
	if cfg == nil {
		cfg = configs.Load()
	}
	r := chi.NewRouter()

	r.Use(middleware.Recovery)
	r.Use(middleware.SecurityHeaders)
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.CORS)
	if cfg.APIKey != "" {
		r.Use(middleware.APIKeyAuth(cfg.APIKey))
	}
	if cfg.RateLimitRPS > 0 && cfg.RateLimitBurst > 0 {
		limiter := middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst)
		r.Use(limiter.Middleware)
	}

	r.Get("/docs", HandleSwaggerUI)
	r.Get("/docs/", HandleSwaggerUI)
	r.Get("/docs/openapi.json", HandleOpenAPISpec)
	r.Get("/swagger", routes.RedirectToDocs)
	r.Get("/swagger/", routes.RedirectToDocs)

	routes.RegisterRoutes(r, ctrl)

	return &Server{
		config:     cfg,
		controller: ctrl,
		handler:    r,
	}
}

// ServeHTTP delegates to the configured router.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
