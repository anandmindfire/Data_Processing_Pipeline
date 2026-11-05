package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/auth"
	"github.com/anandmindfire/data-processing-pipeline/internal/buildinfo"
	"github.com/anandmindfire/data-processing-pipeline/internal/config"
)

// App manages the application runtime lifecycle and graceful shutdown.
type App struct {
	Config       *config.Config
	Dependencies *Dependencies
	Server       *http.Server

	stopBackground context.CancelFunc
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
		MaxHeaderBytes:    64 << 10,
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
	if len(a.Config.APIKeys()) == 0 {
		slog.Warn("API_KEY is not set: the API is unauthenticated; set it for any non-local deployment")
	}
	slog.Info("server listening",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"addr", a.Config.ServerAddr,
		"port", a.Config.ServerPort,
		"api_base_url", a.Config.APIBaseURL,
		"api_base_path", a.Config.APIBasePath,
		"data_dir", a.Config.DataDir,
		"export_dir", a.Config.ExportDir,
		"max_active_jobs", a.Config.MaxActiveJobs,
		"store", a.storeKind(),
		"instance_id", a.instanceID(),
	)

	bgCtx, cancel := context.WithCancel(context.Background())
	a.stopBackground = cancel
	if a.Dependencies != nil && a.Dependencies.Service != nil {
		go a.Dependencies.Service.RunRetention(bgCtx, a.Config.JobRetention, time.Hour)
		go a.Dependencies.Service.Jobs.RunCoordinator(bgCtx)
	}
	if a.Dependencies != nil && a.Dependencies.Keys != nil && a.Config.JobRetention > 0 {
		go runKeyPurge(bgCtx, a.Dependencies.Keys, a.Config.JobRetention)
	}

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
	if a.stopBackground != nil {
		a.stopBackground()
	}
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

func (a *App) storeKind() string {
	if a.Config.UsesPostgres() {
		return "postgres"
	}
	return "sqlite"
}

func (a *App) instanceID() string {
	if a.Dependencies != nil && a.Dependencies.Service != nil {
		return a.Dependencies.Service.Jobs.InstanceID()
	}
	return ""
}

// runKeyPurge deletes API keys that expired or were revoked more than retention ago, hourly.
func runKeyPurge(ctx context.Context, keys *auth.Service, retention time.Duration) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		if n, err := keys.Purge(ctx, time.Now().Add(-retention)); err != nil && ctx.Err() == nil {
			slog.Warn("api key purge failed", "error", err)
		} else if n > 0 {
			slog.Info("purged expired or revoked api keys", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
