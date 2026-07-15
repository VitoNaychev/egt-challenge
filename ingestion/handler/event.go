package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/VitoNaychev/egt-challenge/pkg/correlation"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

//go:generate moq --pkg handler_test -out event_mock_test.go . EventService
type EventService interface {
	Publish(context.Context, service.Event) error
}

// should be used as a singleton instance as per documentation
var validate = validator.New()

type EventHandler struct {
	svc    EventService
	logger *slog.Logger
}

func NewEventHandler(svc EventService, logger *slog.Logger) *EventHandler {
	return &EventHandler{
		svc:    svc,
		logger: logger,
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
	// generate a unique ID that will identify this request's processing path
	correlationID := uuid.New()

	ctx := correlation.NewContext(r.Context(), correlationID.String())
	requestLogger := e.logger.With(slog.String("correlation_id", correlationID.String()))

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

	eventLogger := requestLogger.With(slog.String("event_id", event.ID))

	err = e.svc.Publish(ctx, service.Event{
		ID:      event.ID,
		Message: event.Message,
	})
	if err != nil {
		if errors.Is(err, service.ErrPublishTimeout) {
			eventLogger.Warn("publish timed out", slog.Any("error", err))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		eventLogger.Error("publish failed", slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	eventLogger.Debug("event accepted")
	w.WriteHeader(http.StatusAccepted)
}

func writeErrorResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(ErrorResponse{message})
}
