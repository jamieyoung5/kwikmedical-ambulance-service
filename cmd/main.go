package main

import (
	"github.com/jamieyoung5/kwikmedical-eventstream/pkg/eventutil"
	"go.uber.org/zap"
	"kwikmedical-ambulance-service/handler"
	"os"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		logger.Error("Error initializing logger", zap.Error(err))
		os.Exit(1)
	}

	client, err := eventutil.CreateConnection("localhost:4343")
	if err != nil {
		logger.Error("Error initializing client", zap.Error(err))
		os.Exit(1)
	}

	h := handler.NewHandler(logger, client)

}
