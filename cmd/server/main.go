package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anandkr47/data-processing-pipeline/internal/api"
	"github.com/anandkr47/data-processing-pipeline/internal/config"
	"github.com/anandkr47/data-processing-pipeline/internal/metrics"
	"github.com/anandkr47/data-processing-pipeline/internal/pipeline"
	"github.com/anandkr47/data-processing-pipeline/internal/repository"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	repo, err := repository.NewFileRepository(cfg.DataDir)
	if err != nil {
		slog.Error("initialize repository", "error", err, "dir", cfg.DataDir)
		os.Exit(1)
	}
	defer func() {
		_ = repo.Close()
	}()

	client := &http.Client{Timeout: 30 * time.Second}
	executor := pipeline.NewExecutor(repo, client, cfg.ExportDir, metrics.GlobalCollector)
	mgr := pipeline.NewManager(cfg, repo, executor)
	handler := api.NewServer(cfg, mgr)

	server := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	go func() {
		slog.Info("server listening",
			"addr", cfg.ServerAddr,
			"data_dir", cfg.DataDir,
			"export_dir", cfg.ExportDir,
			"max_active_jobs", cfg.MaxActiveJobs,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	slog.Info("shutting down server...", "signal", sig.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	} else {
		slog.Info("server stopped gracefully")
	}
}
