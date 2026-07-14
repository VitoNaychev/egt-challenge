package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	uniqueViolationCode = "23505"

	defaultQueryTimeout = 2 * time.Second
)

type EventRepository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

type Option func(*EventRepository)

// WithQueryTimeout bounds how long a single repository operation may wait on
// the database before failing with context.DeadlineExceeded.
func WithQueryTimeout(d time.Duration) Option {
	return func(e *EventRepository) {
		e.timeout = d
	}
}

func NewEventRepository(pool *pgxpool.Pool, opts ...Option) *EventRepository {
	e := &EventRepository{
		pool:    pool,
		timeout: defaultQueryTimeout,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *EventRepository) Store(ctx context.Context, event service.Event) error {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	query := `
		INSERT INTO events (id, message)
		VALUES ($1, $2)
	`

	_, err := e.pool.Exec(ctx, query, event.ID, event.Message)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode {
			return service.ErrEventAlreadyExists
		}
		return fmt.Errorf("inserting event: %w", err)
	}
	return nil
}

func (e *EventRepository) Get(ctx context.Context, id string) (service.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	query := `SELECT id, message FROM events WHERE id = $1`

	rows, err := e.pool.Query(ctx, query, id)
	if err != nil {
		return service.Event{}, fmt.Errorf("querying events: %w", err)
	}
	m, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[EventModel])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return service.Event{}, service.ErrEventNotFound
		}
		return service.Event{}, fmt.Errorf("collect event row: %w", err)
	}
	return toDomainEvent(m), nil
}

func (e *EventRepository) List(ctx context.Context) ([]service.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	query := `SELECT id, message FROM events`

	rows, err := e.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}
	ms, err := pgx.CollectRows(rows, pgx.RowToStructByName[EventModel])
	if err != nil {
		return nil, fmt.Errorf("collect event row: %w", err)
	}

	var es []service.Event
	for _, m := range ms {
		es = append(es, toDomainEvent(m))
	}
	return es, nil
}

func toDomainEvent(m EventModel) service.Event {
	return service.Event{
		ID:      m.ID,
		Message: m.Message,
	}
}
