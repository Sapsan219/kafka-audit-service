package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"audit-service/internal/service"
)

type Handler struct {
	audit *service.AuditService
	log   *slog.Logger
}

func New(audit *service.AuditService, log *slog.Logger) *Handler {
	return &Handler{audit: audit, log: log}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /api/audit", h.createAudit)
	return mux
}

type createAuditRequest struct {
	UserID     string          `json:"user_id"`
	Action     string          `json:"action"`
	ResourceID string          `json:"resource_id"`
	Meta       json.RawMessage `json:"meta"`
}

func (h *Handler) createAudit(w http.ResponseWriter, r *http.Request) {
	var request createAuditRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	event, err := h.audit.CreateEvent(
		r.Context(),
		request.UserID,
		request.Action,
		request.ResourceID,
		request.Meta,
	)
	if err != nil {
		if errors.Is(err, service.ErrValidation) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.log.Error("create audit event", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"event_id":  event.EventID,
		"timestamp": event.Timestamp,
	})
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
