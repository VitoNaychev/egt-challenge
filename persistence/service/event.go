package service

import (
	"context"
	"errors"
	"fmt"
)

//go:generate moq --pkg service_test -out event_mock_test.go . EventRepository
type EventRepository interface {
	Store(ctx context.Context, event Event) error
	Get(ctx context.Context, id string) (Event, error)
	List(ctx context.Context) ([]Event, error)
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

func (e *EventService) Get(ctx context.Context, id string) (Event, error) {
	event, err := e.repo.Get(ctx, id)
	if err != nil {
		return Event{}, fmt.Errorf("get event: %w", err)
	}
	return event, nil
}

func (e *EventService) List(ctx context.Context) ([]Event, error) {
	slice, err := e.repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return slice, nil
}
