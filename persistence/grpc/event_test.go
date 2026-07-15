package grpc_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	eventpb "github.com/VitoNaychev/egt-challenge/persistence/gen"
	handler "github.com/VitoNaychev/egt-challenge/persistence/grpc"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEventHandlerGet(t *testing.T) {
	event := service.Event{
		ID:      "example-id",
		Message: "hello, world",
	}

	t.Run("returns event", func(t *testing.T) {
		svc := &EventServiceMock{
			GetFunc: func(ctx context.Context, id string) (service.Event, error) {
				assert.Equal(t, event.ID, id)
				return event, nil
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		resp, err := h.Get(context.Background(), &eventpb.GetRequest{Id: event.ID})
		require.NoError(t, err)

		assert.Equal(t, event.ID, resp.GetEvent().GetId())
		assert.Equal(t, event.Message, resp.GetEvent().GetMessage())
	})

	t.Run("returns InvalidArgument on empty id", func(t *testing.T) {
		svc := &EventServiceMock{}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		_, err := h.Get(context.Background(), &eventpb.GetRequest{})
		require.Error(t, err)

		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Empty(t, svc.GetCalls(), "should not call service on invalid request")
	})

	t.Run("returns NotFound on ErrEventNotFound", func(t *testing.T) {
		svc := &EventServiceMock{
			GetFunc: func(ctx context.Context, id string) (service.Event, error) {
				return service.Event{}, service.ErrEventNotFound
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		_, err := h.Get(context.Background(), &eventpb.GetRequest{Id: event.ID})
		require.Error(t, err)

		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("returns Internal on other errors", func(t *testing.T) {
		svc := &EventServiceMock{
			GetFunc: func(ctx context.Context, id string) (service.Event, error) {
				return service.Event{}, errors.New("example error")
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		_, err := h.Get(context.Background(), &eventpb.GetRequest{Id: event.ID})
		require.Error(t, err)

		assert.Equal(t, codes.Internal, status.Code(err))
	})
}

func TestEventHandlerList(t *testing.T) {
	events := []service.Event{
		{ID: "example-id-1", Message: "hello, world"},
		{ID: "example-id-2", Message: "goodbye, world"},
	}

	t.Run("returns events", func(t *testing.T) {
		svc := &EventServiceMock{
			ListFunc: func(ctx context.Context) ([]service.Event, error) {
				return events, nil
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		resp, err := h.List(context.Background(), &eventpb.ListRequest{})
		require.NoError(t, err)

		require.Len(t, resp.GetEvents(), len(events))
		for i, want := range events {
			assert.Equal(t, want.ID, resp.GetEvents()[i].GetId())
			assert.Equal(t, want.Message, resp.GetEvents()[i].GetMessage())
		}
	})

	t.Run("returns empty list when no events", func(t *testing.T) {
		svc := &EventServiceMock{
			ListFunc: func(ctx context.Context) ([]service.Event, error) {
				return nil, nil
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		resp, err := h.List(context.Background(), &eventpb.ListRequest{})
		require.NoError(t, err)

		assert.Empty(t, resp.GetEvents())
	})

	t.Run("returns Internal on error", func(t *testing.T) {
		svc := &EventServiceMock{
			ListFunc: func(ctx context.Context) ([]service.Event, error) {
				return nil, errors.New("example error")
			},
		}
		h := handler.NewEventHandler(svc, slog.New(slog.DiscardHandler))

		_, err := h.List(context.Background(), &eventpb.ListRequest{})
		require.Error(t, err)

		assert.Equal(t, codes.Internal, status.Code(err))
	})
}
