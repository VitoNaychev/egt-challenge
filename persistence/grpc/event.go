package grpc

import (
	"context"
	"errors"
	"log/slog"

	eventpb "github.com/VitoNaychev/egt-challenge/persistence/gen"
	"github.com/VitoNaychev/egt-challenge/persistence/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate moq --pkg grpc_test -out event_mock_test.go . EventService
type EventService interface {
	Get(ctx context.Context, id string) (service.Event, error)
	List(ctx context.Context) ([]service.Event, error)
}
type EventHandler struct {
	eventpb.UnimplementedEventServiceServer

	svc    EventService
	logger *slog.Logger
}

func NewEventHandler(svc EventService, logger *slog.Logger) *EventHandler {
	return &EventHandler{
		svc:    svc,
		logger: logger,
	}
}

func (e *EventHandler) Get(ctx context.Context, req *eventpb.GetRequest) (*eventpb.GetResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	event, err := e.svc.Get(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, service.ErrEventNotFound) {
			return nil, status.Errorf(codes.NotFound, "event not found")
		}
		e.logger.Error("get event failed", slog.String("id", req.GetId()), slog.Any("error", err))
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	return &eventpb.GetResponse{
		Event: eventToPb(event),
	}, nil
}

func (e *EventHandler) List(ctx context.Context, req *eventpb.ListRequest) (*eventpb.ListResponse, error) {
	events, err := e.svc.List(ctx)
	if err != nil {
		e.logger.Error("list events failed", slog.Any("error", err))
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	pbEvents := []*eventpb.Event{}
	for _, ev := range events {
		pbEvents = append(pbEvents, eventToPb(ev))
	}
	return &eventpb.ListResponse{
		Events: pbEvents,
	}, nil
}

func eventToPb(e service.Event) *eventpb.Event {
	return &eventpb.Event{
		Id:        e.ID,
		SessionId: e.SessionID,
		Type:      e.Type,
		Message:   e.Message,
		Timestamp: timestamppb.New(e.Timestamp),
	}
}
