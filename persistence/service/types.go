package service

import "errors"

var (
	ErrEventAlreadyExists = errors.New("event already exists")
	ErrEventNotFound      = errors.New("event not found")
)

type Event struct {
	ID      string
	Message string
}
