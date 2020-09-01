package service

import (
	"context"
	"net/http"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/identity"
	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	adapter "github.com/ONSdigital/dp-dataset-api/kafka"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	kafka "github.com/ONSdigital/dp-kafka"
	dphandlers "github.com/ONSdigital/dp-net/handlers"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

// check that DatsetAPIStore satifies the the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

//DatsetAPIStore is a wrapper which embeds Neo4j Mongo structs which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	store.MongoDB
	store.GraphDB
}

// Service contains all the configs, server and clients to run the Dataset API
type Service struct {
	Config                    *config.Configuration
	ServiceList               *ExternalServiceList
	GraphDB                   store.GraphDB
	MongoDB                   store.MongoDB
	GenerateDownloadsProducer kafka.IProducer
	identityClient            *clientsidentity.Client
	Server                    HTTPServer
	HealthCheck               HealthChecker
	API                       *api.DatasetAPI
}

// Run the service
func Run(ctx context.Context, cfg *config.Configuration, serviceList *ExternalServiceList, buildTime, gitCommit, version string, svcErrors chan error) (svc *Service, err error) {

	svc = &Service{
		Config:      cfg,
		ServiceList: serviceList,
	}

	// Get MongoDB connection
	svc.MongoDB, err = serviceList.GetMongoDB(ctx, cfg)
	if err != nil {
		log.Event(ctx, "could not obtain mongo session", log.ERROR, log.Error(err))
		return nil, err
	}

	// Get graphDB connection for observation store
	if !cfg.EnableObservationEndpoint && !cfg.EnablePrivateEndpoints {
		log.Event(ctx, "skipping graph DB client creation, because it is not required by the enabled endpoints", log.INFO, log.Data{
			"EnableObservationEndpoint": cfg.EnableObservationEndpoint,
			"EnablePrivateEndpoints":    cfg.EnablePrivateEndpoints,
		})
	} else {
		svc.GraphDB, err = serviceList.GetGraphDB(ctx)
		if err != nil {
			log.Event(ctx, "failed to initialise graph driver", log.FATAL, log.Error(err))
			return nil, err
		}
	}
	store := store.DataStore{Backend: DatsetAPIStore{svc.MongoDB, svc.GraphDB}}

	// Get GenerateDownloads Kafka Producer
	svc.GenerateDownloadsProducer, err = serviceList.GetProducer(ctx, cfg)
	if err != nil {
		log.Event(ctx, "could not obtain generate downloads producer", log.FATAL, log.Error(err))
		return nil, err
	}

	downloadGenerator := &download.Generator{
		Producer:   adapter.NewProducerAdapter(svc.GenerateDownloadsProducer),
		Marshaller: schema.GenerateDownloadsEvent,
	}

	// Get Identity Client (only if private endpoints are enabled)
	if cfg.EnablePrivateEndpoints {
		svc.identityClient = clientsidentity.New(cfg.ZebedeeURL)
	}

	// Get HealthCheck
	svc.HealthCheck, err = serviceList.GetHealthCheck(cfg, buildTime, gitCommit, version)
	if err != nil {
		log.Event(ctx, "could not instantiate healthcheck", log.FATAL, log.Error(err))
		return nil, err
	}
	if err := svc.registerCheckers(ctx); err != nil {
		return nil, errors.Wrap(err, "unable to register checkers")
	}

	// Get HTTP router and server with middleware
	r := mux.NewRouter()
	m := svc.createMiddleware(cfg)
	svc.Server = serviceList.GetHTTPServer(cfg.BindAddr, m.Then(r))

	// Create Dataset API
	urlBuilder := url.NewBuilder(cfg.WebsiteURL)
	datasetPermissions, permissions := getAuthorisationHandlers(ctx, cfg)
	svc.API = api.Setup(ctx, cfg, r, store, urlBuilder, downloadGenerator, datasetPermissions, permissions)

	svc.HealthCheck.Start(ctx)

	// Run the http server in a new go-routine
	go func() {
		if err := svc.Server.ListenAndServe(); err != nil {
			svcErrors <- errors.Wrap(err, "failure in http listen and serve")
		}
	}()

	return svc, nil
}

func getAuthorisationHandlers(ctx context.Context, cfg *config.Configuration) (api.AuthHandler, api.AuthHandler) {
	if !cfg.EnablePermissionsAuth {
		log.Event(ctx, "feature flag not enabled defaulting to nop auth impl", log.INFO, log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
		return &auth.NopHandler{}, &auth.NopHandler{}
	}

	log.Event(ctx, "feature flag enabled", log.INFO, log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
	auth.LoggerNamespace("dp-dataset-api-auth")

	authClient := auth.NewPermissionsClient(dphttp.NewClient())
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

// CreateMiddleware creates an Alice middleware chain of handlers
// to forward collectionID from cookie from header
func (svc *Service) createMiddleware(cfg *config.Configuration) alice.Chain {

	// healthcheck
	healthcheckHandler := newMiddleware(svc.HealthCheck.Handler, "/health")
	middleware := alice.New(healthcheckHandler)

	// Only add the identity middleware when running in publishing.
	if cfg.EnablePrivateEndpoints {
		middleware = middleware.Append(dphandlers.IdentityWithHTTPClient(svc.identityClient))
	}

	// collection ID
	middleware = middleware.Append(dphandlers.CheckHeader(dphandlers.CollectionID))

	return middleware
}

// newMiddleware creates a new http.Handler to intercept /health requests.
func newMiddleware(healthcheckHandler func(http.ResponseWriter, *http.Request), path string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

			if req.Method == "GET" && req.URL.Path == path {
				healthcheckHandler(w, req)
				return
			}

			h.ServeHTTP(w, req)
		})
	}
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.Config.GracefulShutdownTimeout
	log.Event(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout}, log.INFO)
	shutdownContext, cancel := context.WithTimeout(ctx, timeout)
	hasShutdownError := false

	// Gracefully shutdown the application closing any open resources.
	go func() {
		defer cancel()

		// stop healthcheck, as it depends on everything else
		if svc.ServiceList.HealthCheck {
			svc.HealthCheck.Stop()
		}

		// stop any incoming requests
		if err := svc.Server.Shutdown(shutdownContext); err != nil {
			log.Event(shutdownContext, "failed to shutdown http server", log.Error(err), log.ERROR)
			hasShutdownError = true
		}

		// Close MongoDB (if it exists)
		if svc.ServiceList.MongoDB {
			if err := svc.MongoDB.Close(shutdownContext); err != nil {
				log.Event(shutdownContext, "failed to close mongo db session", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
		}

		// Close GenerateDownloadsProducer (if it exists)
		if svc.ServiceList.GenerateDownloadsProducer {
			log.Event(shutdownContext, "closing generated downloads kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
			svc.GenerateDownloadsProducer.Close(shutdownContext)
			log.Event(shutdownContext, "closed generated downloads kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
		}

		// Close GraphDB (if it exists)
		if svc.ServiceList.Graph {
			if err := svc.GraphDB.Close(shutdownContext); err != nil {
				log.Event(shutdownContext, "failed to close graph db", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-shutdownContext.Done()

	// timeout expired
	if shutdownContext.Err() == context.DeadlineExceeded {
		log.Event(shutdownContext, "shutdown timed out", log.ERROR, log.Error(shutdownContext.Err()))
		return shutdownContext.Err()
	}

	// other error
	if hasShutdownError {
		err := errors.New("failed to shutdown gracefully")
		log.Event(shutdownContext, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(shutdownContext, "graceful shutdown was successful", log.INFO)
	return nil
}

// registerCheckers adds the checkers for the provided clients to the health check object
func (svc *Service) registerCheckers(ctx context.Context) (err error) {
	hasErrors := false

	if svc.Config.EnablePrivateEndpoints {
		if err = svc.HealthCheck.AddCheck("Zebedee", svc.identityClient.Checker); err != nil {
			hasErrors = true
			log.Event(ctx, "error adding check for zebedeee", log.ERROR, log.Error(err))
		}
	}

	if err = svc.HealthCheck.AddCheck("Kafka Generate Downloads Producer", svc.GenerateDownloadsProducer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "error adding check for kafka downloads producer", log.ERROR, log.Error(err))
	}

	if err = svc.HealthCheck.AddCheck("Mongo DB", svc.MongoDB.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "error adding check for mongo db", log.ERROR, log.Error(err))
	}

	if svc.Config.EnablePrivateEndpoints || svc.Config.EnableObservationEndpoint {
		log.Event(ctx, "adding graph db health check as the private or observation endpoints are enabled", log.INFO)
		if err = svc.HealthCheck.AddCheck("Graph DB", svc.GraphDB.Checker); err != nil {
			hasErrors = true
			log.Event(ctx, "error adding check for graph db", log.ERROR, log.Error(err))
		}
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}
