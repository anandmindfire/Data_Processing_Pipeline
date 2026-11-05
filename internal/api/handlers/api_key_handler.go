package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/request"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/dto/response"
	"github.com/anandmindfire/data-processing-pipeline/internal/api/respond"
	"github.com/anandmindfire/data-processing-pipeline/internal/auth"
)

const maxKeyRequestBytes = 4 << 10

// APIKeyHandler serves the admin-only API key management endpoints.
type APIKeyHandler struct {
	keys *auth.Service
}

// NewAPIKeyHandler creates the key management handler.
func NewAPIKeyHandler(keys *auth.Service) *APIKeyHandler {
	return &APIKeyHandler{keys: keys}
}

// Create issues a new expiring key. The secret appears only in this response.
func (h *APIKeyHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req request.CreateAPIKeyRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxKeyRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid request JSON: "+err.Error())
		return
	}
	now := time.Now()
	ttl, err := req.TTL(now)
	if err != nil {
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	issued, err := h.keys.Create(r.Context(), req.Name, ttl)
	switch {
	case errors.Is(err, auth.ErrTooManyKeys):
		respond.Error(w, http.StatusConflict, err.Error())
		return
	case err != nil && issued == nil && isValidationError(err):
		respond.Error(w, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		respond.Error(w, http.StatusInternalServerError, "create api key: "+err.Error())
		return
	}
	w.Header().Set("Location", r.URL.Path+"/"+issued.ID)
	respond.JSON(w, http.StatusCreated, response.CreatedAPIKeyResponse{
		APIKeyResponse: response.FromAPIKey(issued.APIKey, now),
		Key:            issued.Key,
		Warning:        "Store this key now. It is shown only once and cannot be retrieved later.",
	})
}

// List returns all keys with their status; secrets are never included.
func (h *APIKeyHandler) List(w http.ResponseWriter, r *http.Request) {
	keys, err := h.keys.List(r.Context())
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "list api keys: "+err.Error())
		return
	}
	now := time.Now()
	out := make([]response.APIKeyResponse, len(keys))
	for i, k := range keys {
		out[i] = response.FromAPIKey(k, now)
	}
	respond.JSON(w, http.StatusOK, out)
}

// Revoke disables a key.
func (h *APIKeyHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	err := h.keys.Revoke(r.Context(), chi.URLParam(r, "keyID"))
	switch {
	case errors.Is(err, auth.ErrKeyNotFound):
		respond.Error(w, http.StatusNotFound, err.Error())
	case err != nil:
		respond.Error(w, http.StatusInternalServerError, "revoke api key: "+err.Error())
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// isValidationError distinguishes request problems from storage failures.
func isValidationError(err error) bool {
	var v *auth.ValidationError
	return errors.As(err, &v)
}
