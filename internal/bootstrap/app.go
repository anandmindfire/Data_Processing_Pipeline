package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/config"
)

// App manages the application runtime lifecycle and graceful shutdown.
type App struct {
	Config       *config.Config
	Dependencies *Dependencies
	Server       *http.Server
}

// NewApp loads configuration, sets up dependencies, and initializes the HTTP server.
func NewApp() (*App, error) {
	cfg := config.Load()
	cfg.InitLogger()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid application config: %w", err)
	}

	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deps, err := BuildDependencies(initCtx, cfg)
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

// Start launches the HTTP server asynchronously. The returned channel receives an error (and is
// closed) if the listener fails, e.g. because the port is already in use, so the caller can exit
// instead of running without serving traffic.
func (a *App) Start() <-chan error {
	slog.Info("server listening",
		"addr", a.Config.ServerAddr,
		"port", a.Config.ServerPort,
		"api_base_url", a.Config.APIBaseURL,
		"api_base_path", a.Config.APIBasePath,
		"data_dir", a.Config.DataDir,
		"export_dir", a.Config.ExportDir,
		"max_active_jobs", a.Config.MaxActiveJobs,
	)

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := a.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	return errCh
}

// Stop shuts down in dependency order: stop accepting HTTP requests, cancel and drain running
// jobs (so they persist a CANCELLED state), then close the store.
func (a *App) Stop(ctx context.Context) error {
	slog.Info("shutting down application server...")
	serverErr := a.Server.Shutdown(ctx)

	var jobsErr error
	if a.Dependencies != nil && a.Dependencies.Service != nil {
		jobsErr = a.Dependencies.Service.Shutdown(ctx)
		if jobsErr != nil {
			slog.Warn("jobs did not stop before the shutdown deadline; they will be marked FAILED on next start", "error", jobsErr)
		}
	}

	if a.Dependencies != nil && a.Dependencies.Store != nil {
		if closeErr := a.Dependencies.Store.Close(); closeErr != nil {
			slog.Warn("failed closing storage repository", "error", closeErr)
		}
	}

	if a.Dependencies != nil && a.Dependencies.Limiter != nil {
		a.Dependencies.Limiter.Close()
	}

	return errors.Join(serverErr, jobsErr)
}
