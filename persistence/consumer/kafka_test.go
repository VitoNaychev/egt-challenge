package consumer_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/consumer"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	eventpb "github.com/VitoNaychev/egt-challenge/pkg/gen"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func oneTickFetch(cancel func(), fn func(ctx context.Context) (kafka.Message, error)) func(ctx context.Context) (kafka.Message, error) {
	firstRun := true
	return func(ctx context.Context) (kafka.Message, error) {
		if firstRun {
			firstRun = false
			return fn(ctx)
		}
		cancel()
		return kafka.Message{}, ctx.Err()
	}
}

func TestKafkaConsumer(t *testing.T) {
	// discard log output during tests
	logger := slog.New(slog.DiscardHandler)

	defaultConfig := consumer.Config{
		UnknownErrorRetryBudget: 3,
		BackoffDuration:         time.Millisecond,
		MaxBackoff:              10 * time.Millisecond,
	}

	event := service.Event{
		ID:        "example-id",
		SessionID: "example-session",
		Type:      "example-type",
		Message:   "hello, world",
		Timestamp: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
	}
	eventBytes, err := proto.Marshal(&eventpb.Event{
		Id:        event.ID,
		SessionId: event.SessionID,
		Type:      event.Type,
		Message:   event.Message,
		Timestamp: timestamppb.New(event.Timestamp),
	})
	require.NoError(t, err, "failed to marshal event")

	msg := kafka.Message{
		Key:   []byte(event.SessionID),
		Value: eventBytes,
	}

	t.Run("process event and commit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				require.Len(t, gotSlice, 1)
				assert.Equal(t, msg, gotSlice[0])

				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				assert.Equal(t, event, got)
				return nil
			},
		}

		cons := consumer.NewKafkaConsumer(defaultConfig, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "did not commit message")
		assert.Len(t, svc.StoreCalls(), 1, "did not process message")
	})

	t.Run("commit message on fail to unmarshal", func(t *testing.T) {
		poisonMessage := kafka.Message{
			Value: []byte{0xde, 0xad, 0xbe, 0xef},
		}

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return poisonMessage, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				require.Len(t, gotSlice, 1)
				assert.Equal(t, poisonMessage, gotSlice[0])

				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return nil
			},
		}

		cons := consumer.NewKafkaConsumer(defaultConfig, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "did not commit message")
		assert.Len(t, svc.StoreCalls(), 0, "should not process invalid message")
	})

	t.Run("ignore service.ErrEventAlreadyExists and commit message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				require.Len(t, gotSlice, 1)
				assert.Equal(t, msg, gotSlice[0])

				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return service.ErrEventAlreadyExists
			},
		}

		cons := consumer.NewKafkaConsumer(defaultConfig, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "did not commit message")
		assert.Len(t, svc.StoreCalls(), 1, "did not process message")
	})

	t.Run("commits message on permanent errors without retring", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				require.Len(t, gotSlice, 1)
				assert.Equal(t, msg, gotSlice[0])

				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return service.NewPermanentError("store event", errors.New("permenent-error"))
			},
		}

		cons := consumer.NewKafkaConsumer(defaultConfig, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "did not commit message")
		assert.Len(t, svc.StoreCalls(), 1, "should process message exactly once")
	})

	t.Run("retries service.RetriableError at least N+1 times", func(t *testing.T) {
		maxRetryAttempts := 5
		storeCallCount := 0

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				require.Len(t, gotSlice, 1)
				assert.Equal(t, msg, gotSlice[0])

				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				storeCallCount++
				if storeCallCount > maxRetryAttempts {
					// cancel context to avoid indefinite retries
					cancel()
				}
				return service.NewRetriableError("store func", errors.New("retriable-error"))
			},
		}

		config := consumer.Config{
			UnknownErrorRetryBudget: maxRetryAttempts,
			BackoffDuration:         0 * time.Second,
		}
		cons := consumer.NewKafkaConsumer(config, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 0, "should not commit message")
		assert.Less(t, maxRetryAttempts, storeCallCount, "did not process message at least N times")
	})

	t.Run("retries unclassified errors N times before commiting message", func(t *testing.T) {
		maxRetryAttempts := 5
		storeCallCount := 0

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				storeCallCount++
				if storeCallCount > maxRetryAttempts+1 {
					// cancel context to avoid indefinite retries
					cancel()
				}
				return errors.New("unknown-error")
			},
		}

		config := consumer.Config{
			UnknownErrorRetryBudget: maxRetryAttempts,
		}
		cons := consumer.NewKafkaConsumer(config, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.NotZero(t, reader.FetchMessageCalls(), "did not call reader")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "should commit message after retring")
		assert.Equal(t, maxRetryAttempts+1, storeCallCount, "should attempt event exactly maxRetryAttempts+1 times (initial call + budget)")
	})

	t.Run("caps retry delay at MaxBackoff", func(t *testing.T) {
		// With 25 retries, uncapped doubling from 1ms sums to over 30s.
		// If the cap is broken, the 1s context deadline expires and the test fails.
		retryBudget := 25

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		reader := &MessageReaderMock{
			FetchMessageFunc: oneTickFetch(cancel, func(ctx context.Context) (kafka.Message, error) {
				return msg, nil
			}),
			CommitMessagesFunc: func(ctx context.Context, gotSlice ...kafka.Message) error {
				return nil
			},
		}
		svc := &EventServiceMock{
			StoreFunc: func(ctx context.Context, got service.Event) error {
				return errors.New("unknown-error")
			},
		}

		config := consumer.Config{
			UnknownErrorRetryBudget: retryBudget,
			BackoffDuration:         time.Millisecond,
			MaxBackoff:              time.Millisecond,
		}
		cons := consumer.NewKafkaConsumer(config, reader, svc, logger)

		err := cons.Run(ctx)
		require.NoError(t, err)

		assert.Len(t, svc.StoreCalls(), retryBudget+1, "should exhaust the full retry budget within the test deadline")
		assert.Len(t, reader.CommitMessagesCalls(), 1, "should commit message after exhausting the budget")
	})
}
