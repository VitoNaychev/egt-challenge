package consumer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/VitoNaychev/egt-challenge/pkg/correlation"
	eventpb "github.com/VitoNaychev/egt-challenge/pkg/gen"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

//go:generate moq --pkg consumer_test -out kafka_mock_test.go . MessageReader EventService
type EventService interface {
	Store(ctx context.Context, ev service.Event) error
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Config struct {
	// bound retries for errors the service layer did not classify
	UnknownErrorRetryBudget int
	// initial delay between retry attempts
	BackoffDuration time.Duration
	// caps the exponentially growing delay between retry attempts
	MaxBackoff time.Duration
}

type KafkaConsumer struct {
	config Config
	reader MessageReader
	svc    EventService
	logger *slog.Logger
}

func NewKafkaConsumer(config Config, reader MessageReader, svc EventService, logger *slog.Logger) *KafkaConsumer {
	return &KafkaConsumer{
		config: config,
		reader: reader,
		svc:    svc,
		logger: logger,
	}
}

func (k *KafkaConsumer) Run(ctx context.Context) error {
	for {
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		logger := k.logger
		// try to add correlation id to logger
		for _, h := range msg.Headers {
			if h.Key == correlation.KafkaHeaderKey {
				logger = logger.With(slog.String("correlation_id", string(h.Value)))
			}
		}

		var eventPb eventpb.Event
		err = proto.Unmarshal(msg.Value, &eventPb)
		if err != nil {
			// poison pill: retrying will never succeed, so log the raw payload
			// for later forensics and commit past it
			logger.Error("dropping message: failed to unmarshal event",
				slog.Any("err", err),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("key", string(msg.Key)),
				slog.String("payload_hex", hex.EncodeToString(msg.Value)),
			)
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("commit message: %w", err)
			}
			continue
		}

		event := service.Event{
			ID:        eventPb.GetId(),
			SessionID: eventPb.GetSessionId(),
			Type:      eventPb.GetType(),
			Message:   eventPb.GetMessage(),
			Timestamp: eventPb.GetTimestamp().AsTime(),
		}
		// matches the "event_id" key the ingestion service logs under, so one
		// grep follows an event across both services
		eventLogger := logger.With(slog.String("event_id", event.ID))

		fn := func(ctx context.Context) error {
			return k.svc.Store(ctx, event)
		}
		err = withExponentialBackOff(ctx, eventLogger, k.config.BackoffDuration, k.config.MaxBackoff, k.config.UnknownErrorRetryBudget, fn)
		switch {
		case err == nil:
			// fallthrough to commit
		case ctx.Err() != nil:
			// fallthrough on context error
		case errors.Is(err, service.ErrEventAlreadyExists):
			// redelivered event: already persisted, safe to commit and move on
			eventLogger.Info("duplicate event, skipping")
		case service.IsPermanent(err):
			// log message payload and commit to avoid stalling the queue
			eventLogger.Error("dropping event: permanent store error", slog.Any("event_data", event), slog.Any("err", err))
		case !service.IsRetriable(err) && !service.IsPermanent(err):
			// log message payload and commit to avoid stalling the queue
			eventLogger.Error("dropping event: retry budget exhausted", slog.Any("event_data", event), slog.Any("err", err))
		default:
			eventLogger.Debug("stored event")
		}

		// should not commit message in case context was canceled
		if ctx.Err() == nil {
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("commit message: %w", err)
			}
		}
	}
}

func withExponentialBackOff(ctx context.Context, logger *slog.Logger, backoff, maxBackoff time.Duration, retryBudget int, fn func(context.Context) error) error {
	err := fn(ctx)
	attempt := 0
	retries := 0
	for err != nil {
		if service.IsPermanent(err) {
			return err
		}
		// event is neither permanent nor retriable
		// therefore it is unclassified - retry up to retryBudget times
		if !service.IsRetriable(err) {
			if retries >= retryBudget {
				return err
			}
			retries++
		}
		attempt++
		logger.Warn("store failed, retrying",
			slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff),
			slog.Any("err", err),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
		err = fn(ctx)
	}
	return nil
}
