package handler

import (
	"context"
	"fmt"
	"github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"go.uber.org/zap"
	"io"
	"log"
)

type Handler struct {
	eventStream pb.EventStreamV1Client
	logger      *zap.Logger
}

func NewHandler(logger *zap.Logger, eventStream pb.EventStreamV1Client) *Handler {
	return &Handler{
		eventStream: eventStream,
		logger:      logger,
	}
}

func (h *Handler) ProcessEvents() error {
	subRequest := &pb.SubscriptionRequest{
		Types:  make([]string, 0),
		Source: "/operator/emergency",
	}

	stream, subErr := h.eventStream.SubscribeToEvents(context.Background(), subRequest)
	if subErr != nil {
		return fmt.Errorf("failed to subscribe to events: %v", subErr)
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			h.logger.Debug("EOF received, event stream closed by server.")
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive events: %v", err)
		}

		h.handleEvent(event)
	}

	return nil
}

func (h *Handler) handleEvent(event *cloudeventspb.CloudEvent) {

	h.logger.Debug("Received event",
		zap.String("Id", event.Id),
		zap.String("Type", event.Type),
		zap.String("Source", event.Source),
	)

	switch event.Type {
	case "type.googleapis.com/EmergencyCall":
		
	default:
		log.Printf("Unhandled event type: %s", event.Type)
	}
}
