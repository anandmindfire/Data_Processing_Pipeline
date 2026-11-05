package respond_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
)

func TestResponseJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	respond.JSON(rec, http.StatusOK, map[string]string{"status": "ok"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %s", ct)
	}
}

func TestResponseError(t *testing.T) {
	recErr := httptest.NewRecorder()
	respond.Error(recErr, http.StatusBadRequest, "bad request")
	if recErr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recErr.Code)
	}
	if !strings.Contains(recErr.Body.String(), `"error":"bad request"`) {
		t.Fatalf("unexpected error body: %s", recErr.Body.String())
	}
}
