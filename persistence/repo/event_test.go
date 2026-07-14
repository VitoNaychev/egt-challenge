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
		ID:      "example-id",
		Message: "hello, world",
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// store event
	err := repo.Store(ctx, event)
	require.NoError(t, err)

	// get event
	got, err := repo.Get(ctx, event.ID)
	require.NoError(t, err)

	assert.Equal(t, event, got)

	// try store same event
	err = repo.Store(ctx, event)
	require.ErrorIs(t, err, service.ErrEventAlreadyExists)

	// try get non existent event
	_, err = repo.Get(ctx, "nonexistent-event")
	require.ErrorIs(t, err, service.ErrEventNotFound)

	second := service.Event{
		ID:      "second-id",
		Message: "goodbye, world",
	}

	// store second event
	err = repo.Store(ctx, second)
	require.NoError(t, err)

	// get all events
	events, err := repo.List(ctx)
	require.NoError(t, err)

	assert.Contains(t, events, event)
	assert.Contains(t, events, second)
}
