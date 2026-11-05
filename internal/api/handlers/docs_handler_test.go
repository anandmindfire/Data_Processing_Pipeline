package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleSwaggerUI(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	rr := httptest.NewRecorder()

	HandleSwaggerUI(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "SwaggerUIBundle") {
		t.Errorf("expected SwaggerUIBundle in body")
	}

	// Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/docs", nil)
	rrPost := httptest.NewRecorder()
	HandleSwaggerUI(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}

func TestHandleOpenAPISpec(t *testing.T) {
	t.Setenv("API_BASE_URL", "https://api.example.com")
	t.Setenv("API_BASE_PATH", "/custom/v2")

	req := httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil)
	rr := httptest.NewRecorder()

	HandleOpenAPISpec(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "https://api.example.com") {
		t.Errorf("expected replaced base URL in spec")
	}
	if !strings.Contains(body, "/custom/v2") {
		t.Errorf("expected custom base path in spec")
	}

	// Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/docs/openapi.json", nil)
	rrPost := httptest.NewRecorder()
	HandleOpenAPISpec(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}
