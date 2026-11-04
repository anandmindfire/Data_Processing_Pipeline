package bootstrap

import (
	"fmt"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/configs"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/controllers"
	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/ingestion"
	"github.com/anandmindfire/data-processing-pipeline/internal/metrics"
	"github.com/anandmindfire/data-processing-pipeline/internal/pipeline"
	"github.com/anandmindfire/data-processing-pipeline/internal/services"
	"github.com/anandmindfire/data-processing-pipeline/internal/storage"
	"github.com/anandmindfire/data-processing-pipeline/internal/transformation"
	"github.com/anandmindfire/data-processing-pipeline/internal/validation"
)

// Dependencies holds initialized services, repositories, and controllers.
type Dependencies struct {
	Config     *configs.Config
	Store      storage.Repository
	Pipeline   *pipeline.Pipeline
	Service    *services.PipelineService
	Controller *controllers.PipelineController
}

// BuildDependencies initializes and wires all application components.
func BuildDependencies(cfg *configs.Config) (*Dependencies, error) {
	store, err := storage.NewFileRepository(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("initialize storage repository: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	pipe := pipeline.NewPipeline(
		ingestion.NewReader(client),
		validation.NewValidator(),
		transformation.NewTransformer(),
		export.NewExporter(cfg.ExportDir),
		store,
		metrics.GlobalCollector,
	)

	service := services.NewPipelineService(cfg, store, pipe)
	controller := controllers.NewPipelineController(service, cfg.MaxRequestBodyBytes)

	return &Dependencies{
		Config:     cfg,
		Store:      store,
		Pipeline:   pipe,
		Service:    service,
		Controller: controller,
	}, nil
}
