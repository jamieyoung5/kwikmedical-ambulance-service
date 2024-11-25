package handler

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jamieyoung5/kwikmedical-db-lib/pkg/client"
	pbSchema "github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"go.uber.org/zap"
	"io"
)

type Handler struct {
	dbClient    *client.KwikMedicalDBClient
	eventStream pbSchema.EventStreamV1Client
	logger      *zap.Logger
}

func NewHandler(logger *zap.Logger, eventStream pbSchema.EventStreamV1Client, dbClient *client.KwikMedicalDBClient) *Handler {
	return &Handler{
		dbClient:    dbClient,
		eventStream: eventStream,
		logger:      logger,
	}
}

func (h *Handler) ProcessEvents() error {
	subRequest := &pbSchema.SubscriptionRequest{
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
		err := h.processEmergencyEvent(event)
		if err != nil {
			h.logger.Error("error while processing emergency event", zap.Error(err), zap.String("Id", event.Id))
		}
	default:
		h.logger.Warn("Unhandled event type", zap.String("event type", event.Type))
	}
}

func (h *Handler) processEmergencyEvent(event *cloudeventspb.CloudEvent) error {
	var emergency pbSchema.EmergencyEvent
	err := proto.Unmarshal(event.GetProtoData().GetValue(), &emergency)
	if err != nil {
		h.logger.Error("Failed to unmarshal EmergencyCall", zap.Error(err))
	}

	return generateNewAmbulanceRequest(h.dbClient, h.eventStream, h.logger, &emergency)
}
