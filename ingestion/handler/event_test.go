package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VitoNaychev/egt-challenge/ingestion/handler"
	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/VitoNaychev/egt-challenge/pkg/correlation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventsHandler_CorrelationID(t *testing.T) {
	handlerEvent := handler.Event{
		ID:      "example-id",
		Message: "hello, world",
	}
	eventJSON, err := json.Marshal(handlerEvent)
	require.NoError(t, err, "failed to marshal event")

	t.Run("propagates correlation_id through context", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(eventJSON))
		resp := httptest.NewRecorder()

		svc := &EventServiceMock{
			PublishFunc: func(ctx context.Context, got service.Event) error {
				correlationID, exists := correlation.FromContext(ctx)

				require.True(t, exists, "did not include correlation id in context")
				assert.NotEmpty(t, correlationID, "correlation ID is empty")
				return nil
			},
		}

		hndl := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))
		hndl.ServeHTTP(resp, req)
	})

	t.Run("generates unique IDs on respective calls", func(t *testing.T) {
		var (
			firstID  string
			secondID string
		)

		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(eventJSON))
		resp := httptest.NewRecorder()

		svc := &EventServiceMock{
			PublishFunc: func(ctx context.Context, got service.Event) error {
				var exists bool
				if firstID == "" {
					firstID, exists = correlation.FromContext(ctx)
					require.True(t, exists, "did not include correlation id in first call")
				} else {
					secondID, exists = correlation.FromContext(ctx)
					require.True(t, exists, "did not include correlation id in second call")
				}

				return nil
			},
		}

		hndl := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))
		hndl.ServeHTTP(resp, req)

		// reinitialize request and response
		req = httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(eventJSON))
		resp = httptest.NewRecorder()

		hndl.ServeHTTP(resp, req)

		assert.NotEqual(t, firstID, secondID, "correlation id collision")
	})
}

func TestEventsHandler_MethodValidation(t *testing.T) {
	handlerEvent := handler.Event{
		ID:      "example-id",
		Message: "hello, world",
	}
	eventJSON, err := json.Marshal(handlerEvent)
	require.NoError(t, err, "failed to marshal event")

	req := httptest.NewRequest(http.MethodPut, "/events", bytes.NewReader(eventJSON))
	resp := httptest.NewRecorder()

	svc := &EventServiceMock{
		PublishFunc: func(ctx context.Context, got service.Event) error {
			t.Error("should not call publish on invalid event")
			return nil
		},
	}

	hndl := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))
	hndl.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}

func TestEventHandler_RequestValidation(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		expected int
	}{
		{
			name:     "returns BadRequest on unmarshal error",
			input:    []byte("invalid-json}}"),
			expected: http.StatusBadRequest,
		},
		{
			name:     "returns BadRequest on missing fields",
			input:    []byte("{\"id\":\"example-id\"}"),
			expected: http.StatusBadRequest,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(c.input))
			resp := httptest.NewRecorder()

			svc := &EventServiceMock{
				PublishFunc: func(ctx context.Context, got service.Event) error {
					return nil
				},
			}

			hndl := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))
			hndl.ServeHTTP(resp, req)

			assert.Equal(t, c.expected, resp.Code)
			assert.Empty(t, svc.PublishCalls(), "publish should not be called")
		})
	}

}

func TestEventHandler_ServiceCases(t *testing.T) {
	want := service.Event{
		ID:      "example-id",
		Message: "hello, world!",
	}
	handlerEvent := handler.Event{
		ID:      want.ID,
		Message: want.Message,
	}
	eventJSON, err := json.Marshal(handlerEvent)
	require.NoError(t, err, "failed to marshal event")

	cases := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "returns Accepted and forwards event to service",
			err:      nil,
			expected: http.StatusAccepted,
		},
		{
			name:     "returns ServiceUnavaliable on service.ErrPublishTimeout",
			err:      service.ErrPublishTimeout,
			expected: http.StatusServiceUnavailable,
		},
		{
			name:     "returns InternalServerError on unsupported error",
			err:      errors.New("unknown error"),
			expected: http.StatusInternalServerError,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(eventJSON))
			resp := httptest.NewRecorder()

			svc := &EventServiceMock{
				PublishFunc: func(ctx context.Context, got service.Event) error {
					assert.Equal(t, want, got)
					return c.err
				},
			}

			hndl := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))
			hndl.ServeHTTP(resp, req)

			assert.Equal(t, c.expected, resp.Code)
			assert.Len(t, svc.PublishCalls(), 1, "did not call service")
		})
	}
}
