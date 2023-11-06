package service

import (
	"context"
	"net/http"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	adapter "github.com/ONSdigital/dp-dataset-api/kafka"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	dphandlers "github.com/ONSdigital/dp-net/v2/handlers"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

// check that DatsetAPIStore satifies the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

// DatsetAPIStore is a wrapper which embeds Neo4j Mongo structs which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	store.MongoDB
}

// Service contains all the configs, server and clients to run the Dataset API
type Service struct {
	config                              *config.Configuration
	serviceList                         *ExternalServiceList
	mongoDB                             store.MongoDB
	generateCantabularDownloadsProducer kafka.IProducer
	identityClient                      *clientsidentity.Client
	server                              HTTPServer
	healthCheck                         HealthChecker
	api                                 *api.DatasetAPI
}

// New creates a new service
func New(cfg *config.Configuration, serviceList *ExternalServiceList) *Service {
	svc := &Service{
		config:      cfg,
		serviceList: serviceList,
	}
	return svc
}

// SetServer sets the http server for a service
func (svc *Service) SetServer(server HTTPServer) {
	svc.server = server
}

// SetHealthCheck sets the healthchecker for a service
func (svc *Service) SetHealthCheck(healthCheck HealthChecker) {
	svc.healthCheck = healthCheck
}

// SetMongoDB sets the mongoDB connection for a service
func (svc *Service) SetMongoDB(mongoDB store.MongoDB) {
	svc.mongoDB = mongoDB
}

// Run the service
func (svc *Service) Run(ctx context.Context, buildTime, gitCommit, version string, svcErrors chan error) (err error) {
	// Get MongoDB connection
	svc.mongoDB, err = svc.serviceList.GetMongoDB(ctx, svc.config.MongoConfig)
	if err != nil {
		log.Error(ctx, "could not obtain mongo session", err)
		return err
	}

	ds := store.DataStore{Backend: DatsetAPIStore{svc.mongoDB}}

	// Get GenerateDownloads Kafka Producer
	if !svc.config.EnablePrivateEndpoints {
		log.Info(ctx, "skipping kafka producer creation, because it is not required by the enabled endpoints", log.Data{
			"EnablePrivateEndpoints": svc.config.EnablePrivateEndpoints,
		})
	} else {
		svc.generateCantabularDownloadsProducer, err = svc.serviceList.GetProducer(ctx, svc.config, svc.config.CantabularExportStartTopic)
		if err != nil {
			log.Fatal(ctx, "could not obtain generate downloads producer for cantabular", err)
			return err
		}
	}

	downloadGeneratorCantabular := &download.CantabularGenerator{
		Producer:   adapter.NewProducerAdapter(svc.generateCantabularDownloadsProducer),
		Marshaller: schema.GenerateCantabularDownloadsEvent,
	}

	downloadGenerators := map[models.DatasetType]api.DownloadsGenerator{
		models.CantabularBlob:              downloadGeneratorCantabular,
		models.CantabularTable:             downloadGeneratorCantabular,
		models.CantabularFlexibleTable:     downloadGeneratorCantabular,
		models.CantabularMultivariateTable: downloadGeneratorCantabular,
	}

	// Get Identity Client (only if private endpoints are enabled)
	if svc.config.EnablePrivateEndpoints {
		svc.identityClient = clientsidentity.New(svc.config.ZebedeeURL)
	}

	// Get HealthCheck
	svc.healthCheck, err = svc.serviceList.GetHealthCheck(svc.config, buildTime, gitCommit, version)
	if err != nil {
		log.Fatal(ctx, "could not instantiate healthcheck", err)
		return err
	}
	if err := svc.registerCheckers(ctx); err != nil {
		return errors.Wrap(err, "unable to register checkers")
	}

	// Get HTTP router and server with middleware
	r := mux.NewRouter()
	m := svc.createMiddleware(svc.config)
	svc.server = svc.serviceList.GetHTTPServer(svc.config.BindAddr, m.Then(r))

	// Create Dataset API
	urlBuilder := url.NewBuilder(svc.config.WebsiteURL)
	datasetPermissions, permissions := getAuthorisationHandlers(ctx, svc.config)
	svc.api = api.Setup(ctx, svc.config, r, ds, urlBuilder, downloadGenerators, datasetPermissions, permissions)

	svc.healthCheck.Start(ctx)

	// Log kafka producer errors in parallel go-routine
	if svc.config.EnablePrivateEndpoints {
		svc.generateCantabularDownloadsProducer.LogErrors(ctx)
	}

	// Run the http server in a new go-routine
	go func() {
		if err := svc.server.ListenAndServe(); err != nil {
			svcErrors <- errors.Wrap(err, "failure in http listen and serve")
		}
	}()

	return nil
}

func getAuthorisationHandlers(ctx context.Context, cfg *config.Configuration) (datasetPermissions, permissions api.AuthHandler) {
	if !cfg.EnablePermissionsAuth {
		log.Info(ctx, "feature flag not enabled defaulting to nop auth impl", log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})
		return &auth.NopHandler{}, &auth.NopHandler{}
	}

	log.Info(ctx, "feature flag enabled", log.Data{"feature": "ENABLE_PERMISSIONS_AUTH"})

	authClient := auth.NewPermissionsClient(dphttp.NewClient())
	authVerifier := auth.DefaultPermissionsVerifier()

	// for checking caller permissions when we have a datasetID, collection ID and user/service token
	datasetPermissions = auth.NewHandler(
		auth.NewDatasetPermissionsRequestBuilder(cfg.ZebedeeURL, "dataset_id", mux.Vars),
		authClient,
		authVerifier,
	)

	// for checking caller permissions when we only have a user/service token
	permissions = auth.NewHandler(
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
	healthcheckHandler := newMiddleware(svc.healthCheck.Handler, "/health")
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
			} else if req.Method == "GET" && req.URL.Path == "/healthcheck" {
				http.NotFound(w, req)
				return
			}

			h.ServeHTTP(w, req)
		})
	}
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.config.GracefulShutdownTimeout
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout})
	shutdownContext, cancel := context.WithTimeout(ctx, timeout)
	hasShutdownError := false

	// Gracefully shutdown the application closing any open resources.
	go func() {
		defer cancel()

		// stop healthcheck, as it depends on everything else
		if svc.serviceList.HealthCheck {
			svc.healthCheck.Stop()
		}

		// stop any incoming requests
		if err := svc.server.Shutdown(shutdownContext); err != nil {
			log.Error(shutdownContext, "failed to shutdown http server", err)
			hasShutdownError = true
		}

		// Close MongoDB (if it exists)
		if svc.serviceList.MongoDB {
			if err := svc.mongoDB.Close(shutdownContext); err != nil {
				log.Error(shutdownContext, "failed to close mongo db session", err)
				hasShutdownError = true
			}
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-shutdownContext.Done()

	// timeout expired
	if shutdownContext.Err() == context.DeadlineExceeded {
		log.Error(shutdownContext, "shutdown timed out", shutdownContext.Err())
		return shutdownContext.Err()
	}

	// other error
	if hasShutdownError {
		err := errors.New("failed to shutdown gracefully")
		log.Error(shutdownContext, "failed to shutdown gracefully ", err)
		return err
	}

	log.Info(shutdownContext, "graceful shutdown was successful")
	return nil
}

// registerCheckers adds the checkers for the provided clients to the health check object
func (svc *Service) registerCheckers(ctx context.Context) (err error) {
	hasErrors := false

	// If feature flag for Publishing mode is disabled then don't do health checks
	if svc.config.EnablePrivateEndpoints {
		log.Info(ctx, "private endpoints enabled: adding kafka and zebedee health checks")
		if err = svc.healthCheck.AddCheck("Zebedee", svc.identityClient.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for zebedeee", err)
		}

		if err = svc.healthCheck.AddCheck("Kafka Generate Cantabular Downloads Producer", svc.generateCantabularDownloadsProducer.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for cantabular kafka downloads producer", err)
		}
	}

	if err = svc.healthCheck.AddCheck("Mongo DB", svc.mongoDB.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for mongo db", err)
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}
