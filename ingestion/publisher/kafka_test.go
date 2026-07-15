package publisher_test

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/VitoNaychev/egt-challenge/ingestion/publisher"
	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/VitoNaychev/egt-challenge/pkg/correlation"
	eventpb "github.com/VitoNaychev/egt-challenge/pkg/gen"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestKafkaPublisher(t *testing.T) {
	event := service.Event{
		ID:        "example-id",
		SessionID: "example-session",
		Type:      "example-type",
		Message:   "hello, world",
		Timestamp: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
	}

	t.Run("encodes event as protobuf and publishes it keyed by session", func(t *testing.T) {
		wantKey := []byte(event.SessionID)

		writer := &MessageWriterMock{
			WriteMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
				require.Len(t, msgs, 1, "no/too many messages")

				msg := msgs[0]
				assert.Equal(t, wantKey, msg.Key, "incorrect key")

				assertProtoEncodedEvent(t, msg, event)

				return nil
			},
		}
		pub := publisher.NewKafkaPublisher(writer)

		err := pub.Publish(context.Background(), event)
		require.NoError(t, err)

		assert.Len(t, writer.WriteMessagesCalls(), 1, "did not write message to queue")
	})

	t.Run("includes correlation ID in message header if it exists", func(t *testing.T) {
		correlationID := "example-correlation-id"

		writer := &MessageWriterMock{
			WriteMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
				require.Len(t, msgs, 1)

				assertCorrelationIDHeader(t, msgs[0], correlationID)
				return nil
			},
		}
		pub := publisher.NewKafkaPublisher(writer)

		ctx := correlation.NewContext(context.Background(), correlationID)

		err := pub.Publish(ctx, event)
		require.NoError(t, err)

	})

	t.Run("returns service.ErrOverloaded on context timeout", func(t *testing.T) {
		writer := &MessageWriterMock{
			WriteMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
				return context.DeadlineExceeded
			},
		}
		pub := publisher.NewKafkaPublisher(writer)

		// create context with instant timeout
		ctx, cancel := context.WithTimeout(context.Background(), 0*time.Second)
		defer cancel()

		err := pub.Publish(ctx, event)

		assert.ErrorIs(t, err, service.ErrPublishTimeout, "did not return service sentinel error")
		assert.Len(t, writer.WriteMessagesCalls(), 1, "did not write message to queue")

	})

	t.Run("returns service.ErrUnavaliable on net.Error", func(t *testing.T) {
		writer := &MessageWriterMock{
			WriteMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
				return &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: syscall.ECONNREFUSED,
				}
			},
		}
		pub := publisher.NewKafkaPublisher(writer)

		err := pub.Publish(context.Background(), event)

		assert.ErrorIs(t, err, service.ErrBrokerUnavailable, "did not return service sentinel error")
		assert.Len(t, writer.WriteMessagesCalls(), 1, "did not write message to queue")
	})

	t.Run("wrapps unsupported error types", func(t *testing.T) {
		wantErr := errors.New("unsupported error")
		writer := &MessageWriterMock{
			WriteMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
				return wantErr
			},
		}
		pub := publisher.NewKafkaPublisher(writer)

		err := pub.Publish(context.Background(), event)

		assert.ErrorIs(t, err, wantErr, "did not wrap original error")
		assert.Len(t, writer.WriteMessagesCalls(), 1, "did not write message to queue")

	})
}

func assertProtoEncodedEvent(t testing.TB, msg kafka.Message, want service.Event) {
	t.Helper()

	var got eventpb.Event
	require.NoError(t, proto.Unmarshal(msg.Value, &got), "message value is not a proto marshalled event")
	assert.Equal(t, want.ID, got.GetId())
	assert.Equal(t, want.SessionID, got.GetSessionId())
	assert.Equal(t, want.Type, got.GetType())
	assert.Equal(t, want.Message, got.GetMessage())
	assert.Equal(t, want.Timestamp, got.GetTimestamp().AsTime())
}

func assertCorrelationIDHeader(t testing.TB, msg kafka.Message, correlationID string) {
	t.Helper()

	require.NotEmpty(t, msg.Headers, "no headers in message")
	for _, h := range msg.Headers {
		if h.Key == correlation.KafkaHeaderKey {
			got := string(h.Value)
			assert.Equal(t, correlationID, got)
			return
		}
	}
	t.Error("correlation id header does not exist")
}
