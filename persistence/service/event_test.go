package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventService(t *testing.T) {
	event := service.Event{
		ID:      "example-id",
		Message: "hello, world",
	}

	t.Run("persists event", func(t *testing.T) {
		repo := &EventRepositoryMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				assert.Equal(t, event, got)
				return nil
			},
		}
		svc := service.NewEventService(repo)

		err := svc.Store(context.Background(), event)
		require.NoError(t, err)

		assert.Len(t, repo.StoreCalls(), 1, "did not persist event")
	})

	t.Run("propagates ErrEventAlreadyExists", func(t *testing.T) {
		repo := &EventRepositoryMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return service.ErrEventAlreadyExists
			},
		}
		svc := service.NewEventService(repo)

		err := svc.Store(context.Background(), event)
		require.ErrorIs(t, err, service.ErrEventAlreadyExists)
	})

	t.Run("wraps other errors from repository", func(t *testing.T) {
		wantErr := errors.New("example error")

		repo := &EventRepositoryMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return wantErr
			},
		}
		svc := service.NewEventService(repo)

		err := svc.Store(context.Background(), event)
		require.ErrorIs(t, err, wantErr)
	})
}
