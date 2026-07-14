package service

import (
	"context"
	"errors"
	"fmt"
)

//go:generate moq --pkg service_test -out event_mock_test.go . EventRepository
type EventRepository interface {
	Store(ctx context.Context, event Event) error
}

type EventService struct {
	repo EventRepository
}

func NewEventService(repo EventRepository) *EventService {
	return &EventService{
		repo: repo,
	}
}

func (e *EventService) Store(ctx context.Context, event Event) error {
	err := e.repo.Store(ctx, event)
	if err != nil && !errors.Is(err, ErrEventAlreadyExists) {
		return fmt.Errorf("store event: %w", err)
	}
	return nil
}
