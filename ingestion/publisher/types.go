package publisher

import "time"

// MUST BE KEPT IN SYNC WITH persistence/consumer/types.go
type Event struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}
