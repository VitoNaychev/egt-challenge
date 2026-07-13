package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/go-playground/validator/v10"
)

//go:generate moq --pkg handler_test -out event_mock_test.go . EventService
type EventService interface {
	Publish(context.Context, service.Event) error
}

// should be used as a singleton instance as per documentation
var validate = validator.New()

type EventHandler struct {
	svc EventService
}

func NewEventHandler(svc EventService) *EventHandler {
	return &EventHandler{
		svc: svc,
	}
}

func (e *EventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		e.handleEvent(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (e *EventHandler) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		writeErrorResponse(w, http.StatusBadRequest, "missing request body")
		return
	}

	var event Event
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "malformed json")
		return
	}

	err = validate.Struct(event)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "invalid event")
		return
	}

	err = e.svc.Publish(r.Context(), service.Event{
		ID:      event.ID,
		Message: event.Message,
	})
	if err != nil {
		if errors.Is(err, service.ErrPublishTimeout) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func writeErrorResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(ErrorResponse{message})
}
