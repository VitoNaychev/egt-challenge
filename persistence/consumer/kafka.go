package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/segmentio/kafka-go"
)

//go:generate moq --pkg consumer_test -out kafka_mock_test.go . MessageReader EventService
type EventService interface {
	Store(ctx context.Context, ev service.Event) error
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type KafkaConsumer struct {
	reader MessageReader
	svc    EventService
}

func NewKafkaConsumer(reader MessageReader, svc EventService) *KafkaConsumer {
	return &KafkaConsumer{
		reader: reader,
		svc:    svc,
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

		var event Event
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			// poison pill: retrying will never succeed, so commit and skip
			slog.Warn("failed to unmarshal event", slog.String("key", string(msg.Key)), slog.Any("err", err))
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("commit message: %w", err)
			}
			continue
		}

		err = k.svc.Store(ctx, service.Event{
			ID:      event.ID,
			Message: event.Message,
		})
		if err != nil {
			// leave uncommitted so the event is redelivered after restart
			return fmt.Errorf("store event %s: %w", event.ID, err)
		}

		if err := k.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
		slog.Debug("stored event", slog.String("id", event.ID))
	}
}
