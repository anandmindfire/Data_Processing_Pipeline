package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
	"github.com/anandmindfire/data-processing-pipeline/internal/buildinfo"
)

// ReadinessCheck reports whether the service can accept traffic (nil = ready).
type ReadinessCheck func(ctx context.Context) error

// Probes serves the Kubernetes-style liveness (/healthz) and readiness (/readyz) endpoints.
type Probes struct {
	ready ReadinessCheck
}

// NewProbes creates probe handlers. ready may be nil (always ready).
func NewProbes(ready ReadinessCheck) *Probes {
	return &Probes{ready: ready}
}

// Live reports that the process is up. It never touches dependencies, so a slow database does
// not cause the orchestrator to restart an otherwise healthy process.
func (p *Probes) Live(w http.ResponseWriter, _ *http.Request) {
	respond.JSON(w, http.StatusOK, map[string]any{"status": "ok", "build": buildinfo.Get()})
}

// Ready reports whether the store is reachable and the service is accepting jobs.
func (p *Probes) Ready(w http.ResponseWriter, r *http.Request) {
	if p.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := p.ready(ctx); err != nil {
			slog.Warn("readiness check failed", "error", err)
			respond.JSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}
	}
	respond.JSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
