package repo_test

import (
	"context"
	"testing"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/repo"
	"github.com/VitoNaychev/egt-challenge/persistence/repo/testutil"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventRepo(t *testing.T) {
	pool, teardown := testutil.SetupPostgres(t)
	defer teardown()

	repo := repo.NewEventRepository(pool)

	event := service.Event{
		ID:        "example-id",
		SessionID: "example-session",
		Type:      "example-type",
		Message:   "hello, world",
		Timestamp: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// store event
	err := repo.Store(ctx, event)
	require.NoError(t, err)

	// get event
	got, err := repo.Get(ctx, event.ID)
	require.NoError(t, err)

	assert.Equal(t, event, normalizeTimestamp(got))

	// try store same event
	err = repo.Store(ctx, event)
	require.ErrorIs(t, err, service.ErrEventAlreadyExists)

	// try get non existent event
	_, err = repo.Get(ctx, "nonexistent-event")
	require.ErrorIs(t, err, service.ErrEventNotFound)

	second := service.Event{
		ID:        "second-id",
		SessionID: "second-session",
		Type:      "second-type",
		Message:   "goodbye, world",
		Timestamp: time.Date(2026, 7, 15, 13, 0, 0, 0, time.UTC),
	}

	// store second event
	err = repo.Store(ctx, second)
	require.NoError(t, err)

	// get all events
	events, err := repo.List(ctx)
	require.NoError(t, err)

	for i, e := range events {
		events[i] = normalizeTimestamp(e)
	}
	assert.Contains(t, events, event)
	assert.Contains(t, events, second)
}

// normalizeTimestamp converts the timestamp to UTC: postgres returns
// TIMESTAMPTZ in the session time zone, which breaks struct equality
// even when the instant is the same.
func normalizeTimestamp(e service.Event) service.Event {
	e.Timestamp = e.Timestamp.UTC()
	return e
}
