package service

import "errors"

var (
	ErrPublishTimeout    = errors.New("failed to publish event due to timeout")
	ErrBrokerUnavailable = errors.New("message broker unavailable")
	ErrFailPublish       = errors.New("failed to publish event")
)

type Event struct {
	ID      string
	Message string
}
