package service

import (
	"errors"
	"time"
)

var (
	ErrEventAlreadyExists = errors.New("event already exists")
	ErrEventNotFound      = errors.New("event not found")
)

type Event struct {
	ID        string
	SessionID string
	Type      string
	Message   string
	Timestamp time.Time
}
