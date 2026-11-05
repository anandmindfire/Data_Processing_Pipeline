// Package api holds the public API contract (OpenAPI 3 specification) of the pipeline service.
// The spec is embedded so the server can serve it at /docs/openapi.json.
package api

import _ "embed"

// OpenAPISpec is the OpenAPI 3.0 document describing the REST API.
//
//go:embed openapi.json
var OpenAPISpec []byte
