package service

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrEventAlreadyExists error = NewPermanentError("event already exists", nil)
	ErrEventNotFound            = errors.New("event not found")
)

// retriable errors can be retried indefinitelly
// until success - e.g. network errors
type RetriableError struct {
	msg string
	err error
}

func NewRetriableError(msg string, err error) *RetriableError {
	return &RetriableError{msg: msg, err: err}
}

func (e *RetriableError) Error() string {
	if e.err == nil {
		return e.msg
	}
	return fmt.Sprintf("%s: %v", e.msg, e.err)
}

func (e *RetriableError) Unwrap() error {
	return e.err
}

func IsRetriable(err error) bool {
	var re *RetriableError
	return errors.As(err, &re)
}

// permanent errors are domain errors that will always
// yeild an error regardless of retrying - e.g. ErrEventAlreadyExists
type PermanentError struct {
	msg string
	err error
}

func NewPermanentError(msg string, err error) *PermanentError {
	return &PermanentError{msg: msg, err: err}
}

func (e *PermanentError) Error() string {
	if e.err == nil {
		return e.msg
	}
	return fmt.Sprintf("%s: %v", e.msg, e.err)
}

func (e *PermanentError) Unwrap() error {
	return e.err
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

type Event struct {
	ID        string
	SessionID string
	Type      string
	Message   string
	Timestamp time.Time
}
