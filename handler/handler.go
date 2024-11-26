package handler

import (
	"github.com/golang/protobuf/proto"
	"github.com/jamieyoung5/kwikmedical-db-lib/pkg/client"
	pbSchema "github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"github.com/jamieyoung5/kwikmedical-eventstream/pkg/eventutil"
	"go.uber.org/zap"
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
		Types: []string{"NewEmergencyCall"},
	}

	return eventutil.Consume(
		h.eventStream,
		h.logger,
		subRequest,
		h.handleEvent,
	)
}

func (h *Handler) handleEvent(event *cloudeventspb.CloudEvent) {

	h.logger.Debug("Received event",
		zap.String("Id", event.Id),
		zap.String("Type", event.Type),
		zap.String("Source", event.Source),
	)

	switch event.Type {
	case "NewEmergencyCall":
		err := h.processEmergencyEvent(event)
		if err != nil {
			h.logger.Error("error while processing emergency event", zap.Error(err), zap.String("Id", event.Id))
		}
	default:
		h.logger.Warn("Unhandled event type", zap.String("event type", event.Type))
	}
}

func (h *Handler) processEmergencyEvent(event *cloudeventspb.CloudEvent) error {
	var emergency pbSchema.EmergencyCall
	err := proto.Unmarshal(event.GetProtoData().GetValue(), &emergency)
	if err != nil {
		h.logger.Error("Failed to unmarshal EmergencyCall", zap.Error(err))
	}

	return generateNewAmbulanceRequest(h.dbClient, h.eventStream, h.logger, &emergency)
}
