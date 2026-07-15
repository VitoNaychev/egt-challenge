package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/VitoNaychev/egt-challenge/ingestion/service"
	"github.com/VitoNaychev/egt-challenge/pkg/correlation"
	"github.com/segmentio/kafka-go"
)

const (
	defaultPublishTimeout = 2 * time.Second
)

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

//go:generate moq --pkg publisher_test -out kafka_mock_test.go . MessageWriter
type KafkaPublisher struct {
	writer  MessageWriter
	timeout time.Duration
}

type Option func(*KafkaPublisher)

func WithPublishTimeout(d time.Duration) Option {
	return func(k *KafkaPublisher) {
		k.timeout = d
	}
}

func NewKafkaPublisher(writer MessageWriter, opts ...Option) *KafkaPublisher {
	k := &KafkaPublisher{
		writer:  writer,
		timeout: defaultPublishTimeout,
	}
	for _, opt := range opts {
		opt(k)
	}
	return k
}

func (k *KafkaPublisher) Publish(ctx context.Context, event service.Event) error {
	var headers []kafka.Header
	if correlationID, exists := correlation.FromContext(ctx); exists {
		headers = append(headers, kafka.Header{
			Key:   correlation.KafkaHeaderKey,
			Value: []byte(correlationID),
		})
	}

	value, err := json.Marshal(Event{
		ID:      event.ID,
		Message: event.Message,
	})
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()

	err = k.writer.WriteMessages(ctx,
		kafka.Message{
			Key:     []byte(event.ID),
			Value:   value,
			Headers: headers,
		},
	)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return service.ErrPublishTimeout
		case errors.As(err, new(net.Error)):
			return service.ErrBrokerUnavailable
		default:
			return fmt.Errorf("write messages: %w", err)
		}
	}
	return nil
}
