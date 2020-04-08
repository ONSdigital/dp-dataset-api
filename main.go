package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-api-clients-go/zebedee"
	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	"github.com/ONSdigital/dp-dataset-api/initialise"
	adapter "github.com/ONSdigital/dp-dataset-api/kafka"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	mongolib "github.com/ONSdigital/dp-mongodb"
	mongoHealth "github.com/ONSdigital/dp-mongodb/health"
	rchttp "github.com/ONSdigital/dp-rchttp"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// check that DatsetAPIStore satifies the the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

//DatsetAPIStore is a wrapper which embeds Neo4j Mongo structs which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	*mongo.Mongo
	*graph.DB
}

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = "dp-dataset-api"
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Event(ctx, "application unexpectedly failed", log.ERROR, log.Error(err))
		os.Exit(1)
	}

	os.Exit(0)
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := config.Get()
	if err != nil {
		log.Event(ctx, "failed to retrieve configuration", log.FATAL, log.Error(err))
		return err
	}

	log.Event(ctx, "config on startup", log.INFO, log.Data{"config": cfg, "build_time": BuildTime, "git-commit": GitCommit})

	// External services and their initialization state
	var serviceList initialise.ExternalServiceList

	var auditor audit.AuditorService
	var auditProducer *kafka.Producer

	if cfg.EnablePrivateEnpoints {
		log.Event(ctx, "private endpoints enabled, enabling action auditing", log.INFO, log.Data{"auditTopicName": cfg.AuditEventsTopic})

		auditProducer, err = serviceList.GetProducer(
			ctx,
			cfg.KafkaAddr,
			cfg.AuditEventsTopic,
			initialise.Audit,
			0,
		)
		if err != nil {
			log.Event(ctx, "could not obtain audit producer", log.FATAL, log.Error(err))
			return err
		}

		auditProducerAdapter := adapter.NewProducerAdapter(auditProducer)
		auditor = audit.New(auditProducerAdapter, "dp-dataset-api")
	} else {
		log.Event(ctx, "private endpoints disabled, auditing will not be enabled", log.INFO)
		auditor = &audit.NopAuditor{}
	}

	mongoClient, mongodb, err := serviceList.GetMongoDB(ctx, cfg)
	if err != nil {
		log.Event(ctx, "could not obtain mongo session", log.ERROR, log.Error(err))
		return err
	}

	// Get graphdb connection for observation store
	graphDB, err := serviceList.GetGraphDB(ctx)
	if err != nil {
		log.Event(ctx, "failed to initialise graph driver", log.FATAL, log.Error(err))
		return err
	}

	store := store.DataStore{Backend: DatsetAPIStore{mongodb, graphDB}}

	generateDownloadsProducer, err := serviceList.GetProducer(
		ctx,
		cfg.KafkaAddr,
		cfg.GenerateDownloadsTopic,
		initialise.GenerateDownloads,
		0,
	)
	if err != nil {
		log.Event(ctx, "could not obtain generate downloads producer", log.FATAL, log.Error(err))
		return err
	}

	downloadGenerator := &download.Generator{
		Producer:   adapter.NewProducerAdapter(generateDownloadsProducer),
		Marshaller: schema.GenerateDownloadsEvent,
	}

	// Get HealthCheck
	hc, err := serviceList.GetHealthCheck(cfg, BuildTime, GitCommit, Version)
	if err != nil {
		log.Event(ctx, "could not instantiate healthcheck", log.FATAL, log.Error(err))
		return err
	}

	zebedeeClient := zebedee.New(cfg.ZebedeeURL)

	// Add dataset API and graph checks
	if err := registerCheckers(ctx, &hc, generateDownloadsProducer, auditProducer, graphDB, mongoClient, zebedeeClient, cfg.EnablePrivateEnpoints); err != nil {
		return err
	}

	apiErrors := make(chan error, 1)

	urlBuilder := url.NewBuilder(cfg.WebsiteURL)

	datasetPermissions, permissions := getAuthorisationHandlers(ctx, cfg)

	hc.Start(ctx)
	api.CreateAndInitialiseDatasetAPI(ctx, *cfg, &hc, store, urlBuilder, apiErrors, downloadGenerator, auditor, datasetPermissions, permissions)

	// block until a fatal error occurs
	select {
	case err := <-apiErrors:
		log.Event(ctx, "api error received", log.ERROR, log.Error(err))
	case <-signals:
		log.Event(ctx, "os signal received", log.INFO)
	}

	log.Event(ctx, fmt.Sprintf("shutdown with timeout: %s", cfg.GracefulShutdownTimeout), log.INFO)
	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

	// track shutdown gracefully closes app
	var gracefulShutdown bool

	// Gracefully shutdown the application closing any open resources.
	go func() {
		defer cancel()
		var hasShutdownError bool

		if serviceList.HealthCheck {
			hc.Stop()
		}

		// stop any incoming requests before closing any outbound connections
		api.Close(shutdownContext)

		if serviceList.MongoDB {
			if err = mongolib.Close(ctx, mongodb.Session); err != nil {
				log.Event(shutdownContext, "failed to close mongo db session", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
		}

		// If generate downloads kafka producer exists, close it
		if serviceList.GenerateDownloadsProducer {
			log.Event(shutdownContext, "closing generated downloads kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
			generateDownloadsProducer.Close(shutdownContext)
			log.Event(shutdownContext, "closed generated downloads kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
		}

		if cfg.EnablePrivateEnpoints {
			// If audit events kafka producer exists, close it
			if serviceList.AuditProducer {
				log.Event(shutdownContext, "closing audit events kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
				auditProducer.Close(shutdownContext)
				log.Event(shutdownContext, "closed audit events kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
			}
		}

		if serviceList.Graph {
			err = graphDB.Close(ctx)
			if err != nil {
				log.Event(ctx, "failed to close graph db", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
		}

		if !hasShutdownError {
			gracefulShutdown = true
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-shutdownContext.Done()

	if !gracefulShutdown {
		err = errors.New("failed to shutdown gracefully")
		log.Event(shutdownContext, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(shutdownContext, "graceful shutdown was successful", log.INFO)

	return nil
}

func getAuthorisationHandlers(ctx context.Context, cfg *config.Configuration) (api.AuthHandler, api.AuthHandler) {
	if !cfg.EnablePermissionsAuth {
		log.Event(ctx, "feature flag not enabled defaulting to nop auth impl", log.INFO, log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
		return &auth.NopHandler{}, &auth.NopHandler{}
	}

	log.Event(ctx, "feature flag enabled", log.INFO, log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
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

// registerCheckers adds the checkers for the provided clients to the healthcheck object
func registerCheckers(ctx context.Context, hc *healthcheck.HealthCheck,
	generateDownloads, auditProducer *kafka.Producer,
	graphDB *graph.DB,
	mongoClient *mongoHealth.Client,
	zebedeeClient *zebedee.Client,
	enablePrivateEnpoints bool,
) (err error) {

	hasErrors := false

	if enablePrivateEnpoints {
		if err = hc.AddCheck("Kafka Audit Producer", auditProducer.Checker); err != nil {
			hasErrors = true
			log.Event(ctx, "error adding check for kafka audit producer", log.ERROR, log.Error(err))
		}

		if err = hc.AddCheck("Zebedee", zebedeeClient.Checker); err != nil {
			hasErrors = true
			log.Event(ctx, "error adding check for zebedeee", log.ERROR, log.Error(err))
		}
	}

	if err = hc.AddCheck("Kafka Generate Downloads Producer", generateDownloads.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "error adding check for kafka downloads producer", log.ERROR, log.Error(err))
	}

	mongoHealth := mongoHealth.CheckMongoClient{
		Client:      *mongoClient,
		Healthcheck: mongoClient.Healthcheck,
	}
	if err = hc.AddCheck("Mongo DB", mongoHealth.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "error adding check for mongo db", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Graph DB", graphDB.Driver.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "error adding check for graph db", log.ERROR, log.Error(err))
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}
