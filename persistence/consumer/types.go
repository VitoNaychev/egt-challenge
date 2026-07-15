package consumer

import "time"

// MUST BE KEPT IN SYNC WITH ingestion/publisher/types.go
type Event struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}
