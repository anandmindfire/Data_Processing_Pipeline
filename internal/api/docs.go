package api

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/http"
	"os"
	"strings"

	pkgresponse "github.com/anandmindfire/data-processing-pipeline/pkg/response"
)

//go:embed openapi.json
var openAPISpec []byte

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Processing Pipeline - Swagger API Documentation</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    body {
      margin: 0;
      background: #fafafa;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    .topbar {
      display: none;
    }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({
        url: "/docs/openapi.json",
        dom_id: "#swagger-ui",
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>`

// HandleSwaggerUI serves the interactive Swagger UI page.
func HandleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		pkgresponse.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// HandleOpenAPISpec serves the raw OpenAPI v3.0 JSON specification with dynamic server URL resolution.
func HandleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		pkgresponse.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	baseURL := os.Getenv("API_BASE_URL")
	if baseURL == "" {
		baseURL = os.Getenv("BASE_URL")
	}
	if baseURL == "" && r != nil && r.Host != "" {
		scheme := "http"
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		baseURL = scheme + "://" + r.Host
	}

	spec := openAPISpec
	if baseURL != "" {
		spec = bytes.Replace(spec, []byte(`"url": "http://localhost:8080"`), fmt.Appendf(nil, `"url": %q`, baseURL), 1)
	}

	basePath := os.Getenv("API_BASE_PATH")
	if basePath == "" {
		basePath = os.Getenv("API_PREFIX")
	}
	if basePath != "" && basePath != "/api/v1" {
		clean := "/" + strings.Trim(basePath, "/")
		spec = bytes.ReplaceAll(spec, []byte(`"/api/v1/`), fmt.Appendf(nil, `"%s/`, clean))
	}

	_, _ = w.Write(spec)
}
