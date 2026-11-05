package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strings"

	apispec "github.com/anandmindfire/data-processing-pipeline/api"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
)

// Swagger UI assets are pinned to an exact version and verified with Subresource Integrity, so a
// compromised or changed CDN file is refused by the browser instead of executing.
const (
	swaggerUIVersion  = "5.34.1"
	swaggerCSSSRI     = "sha384-4c3vsE5f+trhFyHrFrLM1fFGUn531NIaC7dcqEWIJdoRQJ5CkJbSuSCwXLpaS1KF"
	swaggerBundleSRI  = "sha384-XFYN47VzPiDY6qPtxYKg0nqUeW/upo20xf1j2SUE7gT/i35FJB4cuS/71Iegzk8Z"
	swaggerCDN        = "https://unpkg.com/swagger-ui-dist@" + swaggerUIVersion
	swaggerInlineCSS  = `body { margin: 0; background: #fafafa; }`
	swaggerInlineInit = `window.onload = function () {
  SwaggerUIBundle({
    url: "/docs/openapi.json",
    dom_id: "#swagger-ui",
    deepLinking: true,
    presets: [SwaggerUIBundle.presets.apis],
    layout: "BaseLayout"
  });
};`
)

var (
	swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Processing Pipeline - API Documentation</title>
  <link rel="stylesheet" href="` + swaggerCDN + `/swagger-ui.css" integrity="` + swaggerCSSSRI + `" crossorigin="anonymous" />
  <style>` + swaggerInlineCSS + `</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="` + swaggerCDN + `/swagger-ui-bundle.js" integrity="` + swaggerBundleSRI + `" crossorigin="anonymous"></script>
  <script>` + swaggerInlineInit + `</script>
</body>
</html>`

	// swaggerCSP allows only the pinned CDN plus the exact inline style/script (by hash).
	swaggerCSP = "default-src 'none'; " +
		"script-src https://unpkg.com " + cspHash(swaggerInlineInit) + "; " +
		"style-src https://unpkg.com " + cspHash(swaggerInlineCSS) + "; " +
		"img-src 'self' data:; connect-src 'self'; font-src https://unpkg.com; " +
		"frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
)

func cspHash(inline string) string {
	sum := sha256.Sum256([]byte(inline))
	return "'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
}

// HandleSwaggerUI serves the interactive Swagger UI page.
func HandleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		respond.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", swaggerCSP)
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// HandleOpenAPISpec serves the raw OpenAPI v3.0 JSON specification with dynamic server URL resolution.
func HandleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		respond.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}

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

	spec := apispec.OpenAPISpec
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

	_, _ = w.Write(spec) //nolint:gosec // embedded OpenAPI JSON served as application/json, not HTML
}
