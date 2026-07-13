package service

import (
	"context"
	"fmt"
)

//go:generate moq --pkg service_test -out event_mock_test.go . Publisher
type Publisher interface {
	Publish(context.Context, Event) error
}

type EventService struct {
	pub Publisher
}

func NewEventService(pub Publisher) *EventService {
	return &EventService{pub}
}

func (e *EventService) Publish(ctx context.Context, event Event) error {
	err := e.pub.Publish(ctx, event)
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}
