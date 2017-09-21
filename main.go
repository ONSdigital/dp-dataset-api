package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/log"
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

	mongo := &mongo.Mongo{
		Collection: cfg.MongoConfig.Collection,
		Database:   cfg.MongoConfig.Database,
		URI:        cfg.MongoConfig.BindAddr,
	}

	if err := mongo.Init(); err != nil {
		log.ErrorC("Failed to initialise mongo", err, nil)
		os.Exit(1)
	}

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	apiErrors := make(chan error, 1)

	api.CreateDatasetAPI(cfg.DatasetAPIURL, cfg.BindAddr, cfg.SecretKey, store.DataStore{Backend: mongo}, apiErrors)

	// Gracefully shutdown the application closing any open resources.
	gracefulShutdown := func() {
		log.Info(fmt.Sprintf("Shutdown with timeout: %s", cfg.ShutdownTimeout), nil)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)

		api.Close(ctx)

		// mongo.Close() may use all remaining time in the context - do this last!
		if err = mongo.Close(ctx); err != nil {
			log.Error(err, nil)
		}

		log.Info("Shutdown complete", nil)

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
