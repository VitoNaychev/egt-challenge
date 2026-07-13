package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	want := service.Event{
		ID:      "example-id",
		Message: "hello, world",
	}

	t.Run("publishes event to topic", func(t *testing.T) {
		pub := &PublisherMock{
			PublishFunc: func(ctx context.Context, got service.Event) error {
				assert.Equal(t, want, got)
				return nil
			},
		}
		svc := service.NewEventService(pub)
		err := svc.Publish(context.Background(), want)

		assert.NoError(t, err)
		assert.Len(t, pub.PublishCalls(), 1, "did not call publisher")
	})

	t.Run("propagates publish errors", func(t *testing.T) {
		wantErr := errors.New("example-error")

		pub := &PublisherMock{
			PublishFunc: func(ctx context.Context, got service.Event) error {
				return wantErr
			},
		}
		svc := service.NewEventService(pub)
		err := svc.Publish(context.Background(), want)

		assert.ErrorIs(t, err, wantErr)
	})
}
