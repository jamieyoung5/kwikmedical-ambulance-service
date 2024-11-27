package handler

import (
	"github.com/jamieyoung5/kwikmedical-db-lib/pkg/client"
	pbSchema "github.com/jamieyoung5/kwikmedical-eventstream/pb"
	cloudeventspb "github.com/jamieyoung5/kwikmedical-eventstream/pb/io/cloudevents/v1"
	"github.com/jamieyoung5/kwikmedical-eventstream/pkg/eventutil"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func generateNewAmbulanceRequest(
	dbClient *client.KwikMedicalDBClient,
	eventStreamConn pbSchema.EventStreamV1Client,
	logger *zap.Logger,
	emergency *pbSchema.EmergencyCall) error {

	callId, err := dbClient.InsertNewEmergencyCall(emergency)
	if err != nil {
		return err
	}

	hospital, err := dbClient.GetNearestHospital(emergency.Location)
	if err != nil {
		return err
	}

	reqEvent, err := createNewRequestEvent(&pbSchema.AmbulanceRequest{
		EmergencyCallId: callId,
		HospitalId:      int32(hospital.HospitalID),
		Severity:        pbSchema.InjurySeverity_CRITICAL,
		Location:        emergency.Location,
		Status:          pbSchema.RequestStatus_PENDING,
		CreatedAt:       timestamppb.New(time.Now().UTC()),
	})
	if err != nil {
		logger.Error("Failed to create a new Ambulance Request", zap.Error(err))
		return err
	}

	_, err = eventutil.Publish(eventStreamConn, reqEvent, logger)
	if err != nil {
		logger.Error("Failed to publish a new Ambulance Request", zap.Error(err))
		return err
	}

	return nil
}

func createNewRequestEvent(event *pbSchema.AmbulanceRequest) (*cloudeventspb.CloudEvent, error) {

	marshalledEvent, err := proto.Marshal(event)
	if err != nil {
		return nil, err
	}

	return &cloudeventspb.CloudEvent{
		Id:          string(event.EmergencyCallId),
		Source:      "/emergency-processor/ambulance-request",
		SpecVersion: "1.0",
		Type:        "AmbulanceRequest",
		Data: &cloudeventspb.CloudEvent_ProtoData{
			ProtoData: &anypb.Any{
				Value: marshalledEvent,
			},
		},
	}, nil
}
