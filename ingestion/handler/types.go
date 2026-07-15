package handler

import "time"

type Event struct {
	ID        string    `json:"id" validate:"required"`
	SessionID string    `json:"session_id" validate:"required"`
	Type      string    `json:"type" validate:"required"`
	Message   string    `json:"message" validate:"required"`
	Timestamp time.Time `json:"timestamp" validate:"required"`
}

type ErrorResponse struct {
	Msg string `json:"error"`
}
