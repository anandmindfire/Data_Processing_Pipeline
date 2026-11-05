package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/handlers"
)

func TestRegisterRoutesCustomBasePath(t *testing.T) {
	ctrl := &handlers.PipelineHandler{}

	tests := []struct {
		name       string
		basePath   []string
		targetPath string
	}{
		{
			name:       "default prefix when omitted",
			basePath:   nil,
			targetPath: "/api/v1/pipelines",
		},
		{
			name:       "custom prefix with leading and trailing slash handling",
			basePath:   []string{"v2/custom/"},
			targetPath: "/v2/custom/pipelines",
		},
		{
			name:       "root base path",
			basePath:   []string{"/"},
			targetPath: "/pipelines",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chi.NewRouter()
			if tt.basePath != nil {
				RegisterRoutes(r, ctrl, tt.basePath...)
			} else {
				RegisterRoutes(r, ctrl)
			}

			// Verify configured pipeline path route match (options or get)
			req := httptest.NewRequest(http.MethodGet, tt.targetPath, nil)
			rec := httptest.NewRecorder()
			// Will invoke ctrl.List or fail inside handler, but MUST NOT be 404 Not Found
			defer func() { _ = recover() }()
			r.ServeHTTP(rec, req)
			if rec.Code == http.StatusNotFound {
				t.Errorf("expected route %s to be registered, got 404", tt.targetPath)
			}
		})
	}
}

func TestRedirectToDocs(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
	rec := httptest.NewRecorder()
	RedirectToDocs(rec, req)

	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("expected status 301, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/docs" {
		t.Fatalf("expected redirect to /docs, got %s", loc)
	}
}
