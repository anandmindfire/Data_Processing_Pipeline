package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/middleware"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/validator"
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

// Dependencies holds initialized services, repositories, and controllers.
type Dependencies struct {
	Config     *config.Config
	Store      storage.Repository
	Pipeline   *pipeline.Pipeline
	Service    *services.PipelineService
	Controller *controllers.PipelineController
	Limiter    *middleware.RateLimiter
}

// Policy derives the job validation policy (sandboxes and limits) from configuration.
func Policy(cfg *config.Config) validator.Policy {
	return validator.Policy{
		InputDirs:  cfg.InputDirs,
		ExportDir:  cfg.ExportDir,
		MaxWorkers: cfg.MaxWorkersPerStage,
		MaxBuffer:  cfg.MaxBufferSize,
		MaxBatch:   cfg.MaxBatchSize,
		MaxTimeout: cfg.MaxJobTimeoutSeconds,
		MaxSources: cfg.MaxSourcesPerJob,
		MaxExports: cfg.MaxExportsPerJob,
	}
}

// BuildDependencies initializes and wires all application components and marks jobs that a
// previous process left unfinished as FAILED.
func BuildDependencies(ctx context.Context, cfg *config.Config) (*Dependencies, error) {
	store, err := storage.NewSQLiteRepository(ctx, cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("initialize storage repository: %w", err)
	}

	client := ingestion.NewSafeHTTPClient(sourceFetchTimeout, cfg.AllowPrivateSources)
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(client, ingestion.WithPrivateNetworks(cfg.AllowPrivateSources)),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		store,
		metrics.GlobalCollector,
	)

	service := services.NewPipelineService(cfg, store, pipe)
	if n, err := service.Jobs.RecoverInterrupted(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("recover interrupted jobs: %w", err)
	} else if n > 0 {
		slog.Warn("marked jobs interrupted by a previous shutdown as FAILED", "count", n)
	}

	controller := controllers.NewPipelineController(service, cfg.MaxRequestBodyBytes, Policy(cfg))

	var limiter *middleware.RateLimiter
	if cfg.RateLimitRPS > 0 && cfg.RateLimitBurst > 0 {
		limiter = middleware.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst)
	}

	return &Dependencies{
		Config:     cfg,
		Store:      store,
		Pipeline:   pipe,
		Service:    service,
		Controller: controller,
		Limiter:    limiter,
	}, nil
}
