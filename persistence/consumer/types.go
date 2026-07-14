package consumer

// MUST BE KEPT IN SYNC WITH ingestion/publisher/types.go
type Event struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}
