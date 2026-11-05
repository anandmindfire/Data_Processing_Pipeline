package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/handlers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
	"github.com/anandmindfire/data-processing-pipeline/internal/auth"
	"github.com/anandmindfire/data-processing-pipeline/internal/config"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// sourceFetchTimeout bounds a single HTTP source download (the job timeout still applies overall).
const sourceFetchTimeout = 5 * time.Minute

// Dependencies holds initialized services, repositories, and handlers.
type Dependencies struct {
	Config   *config.Config
	Store    storage.Repository
	Pipeline *pipeline.Pipeline
	Service  *services.PipelineService
	Handler  *handlers.PipelineHandler
	Limiter  *middleware.RateLimiter
	Keys     *auth.Service
	KeyAPI   *handlers.APIKeyHandler
	IPs      *middleware.IPResolver
}

// Policy derives the job validation policy (sandboxes and limits) from configuration.
func Policy(cfg *config.Config) validator.Policy {
	return validator.Policy{
		InputDirs:     cfg.InputDirs,
		ExportDir:     cfg.ExportDir,
		MaxWorkers:    cfg.MaxWorkersPerStage,
		MaxBuffer:     cfg.MaxBufferSize,
		MaxBatch:      cfg.MaxBatchSize,
		MaxTimeout:    cfg.MaxJobTimeoutSeconds,
		MaxSources:    cfg.MaxSourcesPerJob,
		MaxExports:    cfg.MaxExportsPerJob,
		MaxAggWorkers: cfg.MaxAggregationWorkers,
	}
}

// OpenStore opens PostgreSQL when DATABASE_URL is set and the embedded SQLite store otherwise.
func OpenStore(ctx context.Context, cfg *config.Config) (*storage.SQLRepository, error) {
	if cfg.UsesPostgres() {
		return storage.NewPostgresRepository(ctx, cfg.DatabaseURL, storage.PostgresOptions{
			MaxOpenConns:    cfg.DBMaxOpenConns,
			MaxIdleConns:    cfg.DBMaxIdleConns,
			ConnMaxLifetime: cfg.DBConnLifetime,
		})
	}
	return storage.NewSQLiteRepository(ctx, cfg.DataDir)
}

// BuildDependencies initializes and wires all application components and marks jobs that a
// previous process left unfinished as FAILED.
func BuildDependencies(ctx context.Context, cfg *config.Config) (*Dependencies, error) {
	store, err := OpenStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("initialize storage repository: %w", err)
	}

	client := ingestion.NewSafeHTTPClient(sourceFetchTimeout, cfg.AllowPrivateSources)
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(client,
			ingestion.WithPrivateNetworks(cfg.AllowPrivateSources),
			ingestion.WithMaxSourceBytes(cfg.MaxSourceBytes)),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		store,
		metrics.GlobalCollector,
	)

	service := services.NewPipelineService(cfg, store, pipe)
	// A SQLite store belongs to this instance alone, so anything left active was abandoned by the
	// previous process. A shared store also holds other replicas' live jobs: only fail jobs whose
	// heartbeat expired.
	cutoff := time.Now()
	if cfg.UsesPostgres() {
		cutoff = cutoff.Add(-cfg.JobLease)
	}
	if n, err := service.Jobs.RecoverInterrupted(ctx, cutoff); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("recover interrupted jobs: %w", err)
	} else if n > 0 {
		slog.Warn("marked jobs interrupted by a previous shutdown as FAILED", "count", n)
	}

	handler := handlers.NewPipelineHandler(service, cfg.MaxRequestBodyBytes, Policy(cfg))

	ips, err := middleware.NewIPResolver(cfg.TrustedProxies)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("invalid TRUSTED_PROXIES: %w", err)
	}
	var limiter *middleware.RateLimiter
	if cfg.RateLimitRPS > 0 && cfg.RateLimitBurst > 0 {
		limiter = middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, ips)
	}

	keys := auth.NewService(store, auth.Options{
		DefaultTTL: cfg.APIKeyDefaultTTL,
		MaxTTL:     cfg.APIKeyMaxTTL,
		MaxActive:  cfg.MaxAPIKeys,
	})

	return &Dependencies{
		Keys:     keys,
		KeyAPI:   handlers.NewAPIKeyHandler(keys),
		Config:   cfg,
		Store:    store,
		Pipeline: pipe,
		Service:  service,
		Handler:  handler,
		Limiter:  limiter,
		IPs:      ips,
	}, nil
}
