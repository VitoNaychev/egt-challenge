package repo

import "time"

type EventModel struct {
	ID        string    `db:"id"`
	SessionID string    `db:"session_id"`
	Type      string    `db:"type"`
	Message   string    `db:"message"`
	Timestamp time.Time `db:"timestamp"`
}
