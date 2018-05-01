package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/audit"

	"github.com/ONSdigital/go-ns/kafka"

	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/log"
	mongoclosure "github.com/ONSdigital/go-ns/mongo"
	"github.com/pkg/errors"
)

func main() {
	log.Namespace = "dp-dataset-api"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	log.Info("config on startup", log.Data{"config": cfg})

	generateDownloadsProducer, err := kafka.NewProducer(cfg.KafkaAddr, cfg.GenerateDownloadsTopic, 0)
	if err != nil {
		log.Error(errors.Wrap(err, "error creating kakfa producer"), nil)
		os.Exit(1)
	}

	// TODO replace with real implementation when ready
	auditor := &audit.NopAuditor{}

	mongo := &mongo.Mongo{
		CodeListURL: cfg.CodeListAPIURL,
		Collection:  cfg.MongoConfig.Collection,
		Database:    cfg.MongoConfig.Database,
		DatasetURL:  cfg.DatasetAPIURL,
		URI:         cfg.MongoConfig.BindAddr,
	}

	session, err := mongo.Init()
	if err != nil {
		log.ErrorC("failed to initialise mongo", err, nil)
		os.Exit(1)
	}

	mongo.Session = session

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	store := store.DataStore{Backend: mongo}

	downloadGenerator := &download.Generator{
		Producer:   generateDownloadsProducer,
		Marshaller: schema.GenerateDownloadsEvent,
	}

	apiErrors := make(chan error, 1)

	urlBuilder := url.NewBuilder(cfg.WebsiteURL)

	api.CreateDatasetAPI(*cfg, store, urlBuilder, apiErrors, downloadGenerator, auditor)

	// Gracefully shutdown the application closing any open resources.
	gracefulShutdown := func() {
		log.Info(fmt.Sprintf("shutdown with timeout: %s", cfg.GracefulShutdownTimeout), nil)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

		// stop any incoming requests before closing any outbound connections
		api.Close(ctx)

		if err = mongoclosure.Close(ctx, session); err != nil {
			log.Error(err, nil)
		}

		if err := generateDownloadsProducer.Close(ctx); err != nil {
			log.Error(errors.Wrap(err, "error while attempting to shutdown kafka producer"), nil)
		}

		log.Info("shutdown complete", nil)

		cancel()
		os.Exit(1)
	}

	for {
		select {
		case err := <-apiErrors:
			log.ErrorC("api error received", err, nil)
			gracefulShutdown()
		case <-signals:
			log.Debug("os signal received", nil)
			gracefulShutdown()
		}
	}
}
