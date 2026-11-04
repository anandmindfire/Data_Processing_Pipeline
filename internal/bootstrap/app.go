package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/configs"
)

// App manages the application runtime lifecycle and graceful shutdown.
type App struct {
	Config       *configs.Config
	Dependencies *Dependencies
	Server       *http.Server
}

// NewApp loads configuration, sets up dependencies, and initializes the HTTP server.
func NewApp() (*App, error) {
	cfg := configs.Load()
	cfg.InitLogger()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid application config: %w", err)
	}

	deps, err := BuildDependencies(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed initializing dependencies: %w", err)
	}

	router := BuildRouter(deps)

	server := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	return &App{
		Config:       cfg,
		Dependencies: deps,
		Server:       server,
	}, nil
}

// Start launches the HTTP server asynchronously.
func (a *App) Start() {
	slog.Info("server listening",
		"addr", a.Config.ServerAddr,
		"port", a.Config.ServerPort,
		"api_base_url", a.Config.APIBaseURL,
		"api_base_path", a.Config.APIBasePath,
		"data_dir", a.Config.DataDir,
		"export_dir", a.Config.ExportDir,
		"max_active_jobs", a.Config.MaxActiveJobs,
	)

	go func() {
		if err := a.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
		}
	}()
}

// Stop gracefully terminates the HTTP server and flushes repository handles.
func (a *App) Stop(ctx context.Context) error {
	slog.Info("shutting down application server...")
	serverErr := a.Server.Shutdown(ctx)

	if a.Dependencies != nil && a.Dependencies.Store != nil {
		if closeErr := a.Dependencies.Store.Close(); closeErr != nil {
			slog.Warn("failed closing storage repository", "error", closeErr)
		}
	}

	return serverErr
}
