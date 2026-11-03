package main

import (
	"context"
	"errors"
	"github.com/anandkr47/data-processing-pipeline/internal/api"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	dir := env("DATA_DIR", "data")
	store, err := storage.New(dir)
	if err != nil {
		slog.Error("initialize storage", "error", err)
		os.Exit(1)
	}
	addr := env("ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: api.New(store), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		slog.Info("server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown failed", "error", err)
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
