package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/service"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

const serviceName = "dp-dataset-api"

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Event(ctx, "application unexpectedly failed", log.ERROR, log.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Create the service, providing an error channel for fatal errors
	svcErrors := make(chan error, 1)
	svcList := service.NewServiceList(&service.Init{})
	// Read config
	cfg, err := config.Get()
	if err != nil {
		log.Event(ctx, "failed to retrieve configuration", log.FATAL, log.Error(err))
		return err
	}
	log.Event(ctx, "config on startup", log.INFO, log.Data{"config": cfg, "build_time": BuildTime, "git-commit": GitCommit})

	// Run the service
	svc := service.New(cfg, svcList)
	if err := svc.Run(ctx, BuildTime, GitCommit, Version, svcErrors); err != nil {
		return errors.Wrap(err, "running service failed")
	}

	// Blocks until an os interrupt or a fatal error occurs
	select {
	case err := <-svcErrors:
		log.Event(ctx, "service error received", log.ERROR, log.Error(err))
	case sig := <-signals:
		log.Event(ctx, "os signal received", log.Data{"signal": sig}, log.INFO)
	}
	return svc.Close(ctx)
}
