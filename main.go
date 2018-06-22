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
	"github.com/ONSdigital/dp-dataset-api/neo4j"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-filter/observation"

	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/healthcheck"
	"github.com/ONSdigital/go-ns/kafka"
	neo4jhealth "github.com/ONSdigital/go-ns/neo4j"

	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/log"
	mongolib "github.com/ONSdigital/go-ns/mongo"
	"github.com/pkg/errors"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

var _ store.Storer = DatsetAPIStore{}

type DatsetAPIStore struct {
	*mongo.Mongo
	*neo4j.Neo4j
}

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

	var auditor audit.AuditorService
	var auditProducer kafka.Producer

	if cfg.EnablePrivateEnpoints {
		log.Info("private endpoints enabled, enabling action auditing", log.Data{"auditTopicName": cfg.AuditEventsTopic})

		auditProducer, err = kafka.NewProducer(cfg.KafkaAddr, cfg.AuditEventsTopic, 0)
		if err != nil {
			log.Error(errors.Wrap(err, "error creating kakfa audit producer"), nil)
			os.Exit(1)
		}

		auditor = audit.New(auditProducer, "dp-dataset-api")
	} else {
		log.Info("private endpoints disabled, auditing will not be enabled", nil)
		auditor = &audit.NopAuditor{}
	}

	mongodb := &mongo.Mongo{
		CodeListURL: cfg.CodeListAPIURL,
		Collection:  cfg.MongoConfig.Collection,
		Database:    cfg.MongoConfig.Database,
		DatasetURL:  cfg.DatasetAPIURL,
		URI:         cfg.MongoConfig.BindAddr,
	}

	session, err := mongodb.Init()
	if err != nil {
		log.ErrorC("failed to initialise mongo", err, nil)
		os.Exit(1)
	}

	mongodb.Session = session

	log.Debug("listening...", log.Data{
		"bind_address": cfg.BindAddr,
	})

	neo4jConnPool, err := bolt.NewClosableDriverPool(cfg.Neo4jBindAddress, cfg.Neo4jPoolSize)
	if err != nil {
		log.ErrorC("failed to connect to neo4j connection pool", err, nil)
		os.Exit(1)
	}

	neoDB := &neo4j.Neo4j{neo4jConnPool}

	store := store.DataStore{Backend: DatsetAPIStore{Mongo: mongodb, Neo4j: neoDB}}

	observationStore := observation.NewStore(neo4jConnPool)

	downloadGenerator := &download.Generator{
		Producer:   generateDownloadsProducer,
		Marshaller: schema.GenerateDownloadsEvent,
	}

	healthTicker := healthcheck.NewTicker(
		cfg.HealthCheckInterval,
		neo4jhealth.NewHealthCheckClient(neo4jConnPool),
		mongolib.NewHealthCheckClient(mongodb.Session),
	)

	apiErrors := make(chan error, 1)

	urlBuilder := url.NewBuilder(cfg.WebsiteURL)

	api.CreateDatasetAPI(*cfg, store, urlBuilder, apiErrors, downloadGenerator, auditor, observationStore)

	// Gracefully shutdown the application closing any open resources.
	gracefulShutdown := func() {
		log.Info(fmt.Sprintf("shutdown with timeout: %s", cfg.GracefulShutdownTimeout), nil)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

		// stop any incoming requests before closing any outbound connections
		api.Close(ctx)

		healthTicker.Close()

		if err = mongolib.Close(ctx, session); err != nil {
			log.Error(err, nil)
		}

		if err = neo4jConnPool.Close(); err != nil {
			log.Error(err, nil)
		}

		if err = generateDownloadsProducer.Close(ctx); err != nil {
			log.Error(errors.Wrap(err, "error while attempting to shutdown kafka producer"), nil)
		}

		if cfg.EnablePrivateEnpoints {
			log.Debug("exiting audit producer", nil)
			if err = auditProducer.Close(ctx); err != nil {
				log.Error(err, nil)
			}
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
