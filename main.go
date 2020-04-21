package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/dp-graph/graph"
	rchttp "github.com/ONSdigital/dp-rchttp"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/healthcheck"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	mongolib "github.com/ONSdigital/go-ns/mongo"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// check that DatsetAPIStore satifies the the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

//DatsetAPIStore is a wrapper which embeds Neo4j Mongo stucts which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	*mongo.Mongo
	*graph.DB
}

type initialisedStruct struct {
	generateDownloadsProducer bool
	auditProducer             bool
	mongo                     bool
	healthTicker              bool
}

var initialised = initialisedStruct{
	generateDownloadsProducer: true,
	auditProducer:             true,
	mongo:                     true,
	healthTicker:              true,
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

	defer func() {
		if x := recover(); x != nil {
			// Capture run time panic's in the log ...
			log.Error(errors.New(fmt.Sprintf("PANIC: %+v", x)), nil)
		}
	}()

	log.Info("config on startup", log.Data{"config": cfg})

	generateDownloadsProducer, err := kafka.NewProducer(cfg.KafkaAddr, cfg.GenerateDownloadsTopic, 0)
	if err != nil {
		log.Error(errors.Wrap(err, "error creating kakfa producer"), nil)
		initialised.generateDownloadsProducer = false
	}

	var auditor audit.AuditorService
	var auditProducer kafka.Producer

	if cfg.EnablePrivateEnpoints {
		log.Info("private endpoints enabled, enabling action auditing", log.Data{"auditTopicName": cfg.AuditEventsTopic})

		auditProducer, err = kafka.NewProducer(cfg.KafkaAddr, cfg.AuditEventsTopic, 0)
		if err != nil {
			log.Error(errors.Wrap(err, "error creating kakfa audit producer"), nil)
			initialised.auditProducer = false
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
		initialised.mongo = false
	} else {
		mongodb.Session = session
		log.Debug("listening...", log.Data{
			"bind_address": cfg.BindAddr,
		})
	}

	graphDB, err := graph.New(context.Background(), graph.Subsets{Observation: true, Instance: true})
	if err != nil {
		log.ErrorC("failed to initialise graph driver", err, nil)
	}

	store := store.DataStore{Backend: DatsetAPIStore{mongodb, graphDB}}

	downloadGenerator := &download.Generator{
		Producer:   generateDownloadsProducer,
		Marshaller: schema.GenerateDownloadsEvent,
	}

	var healthyClients []healthcheck.Client
	healthyClients = append(healthyClients, *graphDB)
	if initialised.mongo {
		healthyClients = append(healthyClients, mongolib.NewHealthCheckClient(mongodb.Session))
	}

	// Only apply a healthticker where the clients are healthy
	var healthTicker *healthcheck.Ticker
	if len(healthyClients) != 0 {
		healthTicker = healthcheck.NewTicker(
			cfg.HealthCheckInterval,
			cfg.HealthCheckRecoveryInterval,
			healthyClients...,
		)
	} else {
		initialised.healthTicker = false
	}

	apiErrors := make(chan error, 1)

	urlBuilder := url.NewBuilder(cfg.WebsiteURL)

	datasetPermissions, permissions := getAuthorisationHandlers(cfg)

	api.CreateAndInitialiseDatasetAPI(*cfg, store, urlBuilder, apiErrors, downloadGenerator, auditor, datasetPermissions, permissions)

	// Gracefully shutdown the application closing any open resources.
	gracefulShutdown := func() {
		log.Info(fmt.Sprintf("shutdown with timeout: %s", cfg.GracefulShutdownTimeout), nil)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

		// stop any incoming requests before closing any outbound connections
		api.Close(ctx)

		if initialised.healthTicker {
			healthTicker.Close()
		}

		if initialised.mongo {
			if err = mongolib.Close(ctx, session); err != nil {
				log.Error(err, nil)
			}
		}

		if err = graphDB.Close(ctx); err != nil {
			log.Error(err, nil)
		}

		if initialised.generateDownloadsProducer {
			if err = generateDownloadsProducer.Close(ctx); err != nil {
				log.Error(errors.Wrap(err, "error while attempting to shutdown kafka producer"), nil)
			}
		}

		if cfg.EnablePrivateEnpoints {
			log.Debug("exiting audit producer", nil)
			if initialised.auditProducer {
				if err = auditProducer.Close(ctx); err != nil {
					log.Error(err, nil)
				}
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
		case <-signals:
			log.Debug("os signal received", nil)
			gracefulShutdown()
		}
	}
}

func getAuthorisationHandlers(cfg *config.Configuration) (api.AuthHandler, api.AuthHandler) {
	if !cfg.EnablePermissionsAuth {
		log.Info("feature flag not enabled defaulting to nop auth impl", log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
		return &auth.NopHandler{}, &auth.NopHandler{}
	}

	log.Info("feature flag enabled", log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
	auth.LoggerNamespace("dp-dataset-api-auth")

	authClient := auth.NewPermissionsClient(rchttp.NewClient())
	authVerifier := auth.DefaultPermissionsVerifier()

	// for checking caller permissions when we have a datasetID, collection ID and user/service token
	datasetPermissions := auth.NewHandler(
		auth.NewDatasetPermissionsRequestBuilder(cfg.ZebedeeURL, "dataset_id", mux.Vars),
		authClient,
		authVerifier,
	)

	// for checking caller permissions when we only have a user/service token
	permissions := auth.NewHandler(
		auth.NewPermissionsRequestBuilder(cfg.ZebedeeURL),
		authClient,
		authVerifier,
	)

	return datasetPermissions, permissions
}
