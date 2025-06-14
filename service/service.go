package service

import (
	"context"
	"net/http"
	neturl "net/url"
	"slices"
	"sync"

	"github.com/ONSdigital/dp-api-clients-go/v2/files"
	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/api"
	"github.com/ONSdigital/dp-dataset-api/application"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	adapter "github.com/ONSdigital/dp-dataset-api/kafka"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	dphandlers "github.com/ONSdigital/dp-net/v3/handlers"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// check that DatsetAPIStore satifies the store.Storer interface
var _ store.Storer = (*DatsetAPIStore)(nil)

// DatsetAPIStore is a wrapper which embeds Neo4j Mongo structs which between them satisfy the store.Storer interface.
type DatsetAPIStore struct {
	store.MongoDB
	store.GraphDB
}

// Service contains all the configs, server and clients to run the Dataset API
type Service struct {
	config                              *config.Configuration
	serviceList                         *ExternalServiceList
	graphDB                             store.GraphDB
	graphDBErrorConsumer                Closer
	mongoDB                             store.MongoDB
	generateCMDDownloadsProducer        kafka.IProducer
	generateCantabularDownloadsProducer kafka.IProducer
	identityClient                      *clientsidentity.Client
	filesAPIClient                      *files.Client
	server                              HTTPServer
	healthCheck                         HealthChecker
	api                                 *api.DatasetAPI
	smDS                                *application.StateMachineDatasetAPI
}

var stateMachine *application.StateMachine
var stateMachineInit sync.Once

func GetListTransitions() []application.Transition {
	publishedTransition := application.Transition{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"created", "associated", "published"},
		Type:                "v4",
	}

	associatedTransition := application.Transition{
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "v4",
	}

	edconfirmedTransition := application.Transition{
		Label:               "edition-confirmed",
		TargetState:         application.EditionConfirmed,
		AllowedSourceStates: []string{"completed", "edition-confirmed"},
		Type:                "v4",
	}

	return []application.Transition{publishedTransition,
		associatedTransition, edconfirmedTransition}
}

func GetListStaticTransitions() []application.Transition {
	publishedTransition := application.Transition{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"approved"},
		Type:                "static",
	}

	associatedTransition := application.Transition{
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"created", "associated"},
		Type:                "static",
	}

	approvedTransition := application.Transition{
		Label:               "approved",
		TargetState:         application.Approved,
		AllowedSourceStates: []string{"associated"},
		Type:                "static",
	}

	return []application.Transition{publishedTransition,
		associatedTransition, approvedTransition}
}

func GetListCantabularTransitions() []application.Transition {
	publishedTransition := application.Transition{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"published", "associated", "edition-confirmed"},
		Type:                "cantabular_flexible_table",
	}

	associatedTransition := application.Transition{
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "cantabular_flexible_table",
	}

	edconfirmedTransition := application.Transition{
		Label:               "edition-confirmed",
		TargetState:         application.EditionConfirmed,
		AllowedSourceStates: []string{"completed", "edition-confirmed"},
		Type:                "cantabular_flexible_table",
	}

	return []application.Transition{publishedTransition,
		associatedTransition, edconfirmedTransition}
}

func GetListMultivariateCantabularTransitions() []application.Transition {
	publishedTransition := application.Transition{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"associated", "edition-confirmed", "published"},
		Type:                "cantabular_multivariate_table",
	}

	associatedTransition := application.Transition{
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "cantabular_multivariate_table",
	}

	edconfirmedTransition := application.Transition{
		Label:               "edition-confirmed",
		TargetState:         application.EditionConfirmed,
		AllowedSourceStates: []string{"completed", "edition-confirmed"},
		Type:                "cantabular_multivariate_table",
	}

	return []application.Transition{publishedTransition,
		associatedTransition, edconfirmedTransition}
}

func GetStateMachine(ctx context.Context, dataStore store.DataStore) *application.StateMachine {
	stateMachineInit.Do(func() {
		states := []application.State{application.Published, application.EditionConfirmed, application.Associated}
		transitions := slices.Concat(GetListTransitions(), GetListCantabularTransitions(), GetListMultivariateCantabularTransitions(), GetListStaticTransitions())
		stateMachine = application.NewStateMachine(ctx, states, transitions, dataStore)
	})
	return stateMachine
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

// SetDownloadsProducer sets the downloads kafka producer for a service
func (svc *Service) SetDownloadsProducer(producer kafka.IProducer) {
	svc.generateCMDDownloadsProducer = producer
}

// SetMongoDB sets the mongoDB connection for a service
func (svc *Service) SetMongoDB(mongoDB store.MongoDB) {
	svc.mongoDB = mongoDB
}

// SetGraphDB sets the graphDB connection for a service
func (svc *Service) SetGraphDB(graphDB store.GraphDB) {
	svc.graphDB = graphDB
}

// SetGraphDBErrorConsumer sets the graphDB error consumer for a service
func (svc *Service) SetGraphDBErrorConsumer(graphDBErrorConsumer Closer) {
	svc.graphDBErrorConsumer = graphDBErrorConsumer
}

// SetFilesAPIClient sets the files API client for a service
func (svc *Service) SetFilesAPIClient(filesAPIClient *files.Client) {
	svc.filesAPIClient = filesAPIClient
}

// Run the service
func (svc *Service) Run(ctx context.Context, buildTime, gitCommit, version string, svcErrors chan error) (err error) {
	// Copilot used to move initMongoDB and initGraphDB functions out of Run
	if err := svc.initMongoDB(ctx); err != nil {
		return err
	}

	if err := svc.initGraphDB(ctx); err != nil {
		return err
	}

	ds := store.DataStore{Backend: DatsetAPIStore{svc.mongoDB, svc.graphDB}}

	// Get GenerateDownloads Kafka Producer
	if !svc.config.EnablePrivateEndpoints {
		log.Info(ctx, "skipping kafka producer creation, because it is not required by the enabled endpoints", log.Data{
			"EnablePrivateEndpoints": svc.config.EnablePrivateEndpoints,
		})
	} else {
		svc.generateCMDDownloadsProducer, err = svc.serviceList.GetProducer(ctx, svc.config, svc.config.GenerateDownloadsTopic)
		if err != nil {
			log.Fatal(ctx, "could not obtain generate downloads producer for CMD", err)
			return err
		}
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

	downloadGeneratorCMD := &download.CMDGenerator{
		Producer:   adapter.NewProducerAdapter(svc.generateCMDDownloadsProducer),
		Marshaller: schema.GenerateCMDDownloadsEvent,
	}

	downloadGenerators := map[models.DatasetType]api.DownloadsGenerator{
		models.CantabularBlob:              downloadGeneratorCantabular,
		models.CantabularTable:             downloadGeneratorCantabular,
		models.CantabularFlexibleTable:     downloadGeneratorCantabular,
		models.CantabularMultivariateTable: downloadGeneratorCantabular,
		models.Filterable:                  downloadGeneratorCMD,
	}

	smDownloadGenerators := map[models.DatasetType]application.DownloadsGenerator{
		models.CantabularBlob:              downloadGeneratorCantabular,
		models.CantabularTable:             downloadGeneratorCantabular,
		models.CantabularFlexibleTable:     downloadGeneratorCantabular,
		models.CantabularMultivariateTable: downloadGeneratorCantabular,
		models.Filterable:                  downloadGeneratorCMD,
	}

	// Get Identity Client (only if private endpoints are enabled)
	if svc.config.EnablePrivateEndpoints {
		svc.identityClient = clientsidentity.New(svc.config.ZebedeeURL)
	}

	// Initialise Files API Client (if private endpoints are enabled)
	if svc.config.EnablePrivateEndpoints {
		if svc.filesAPIClient == nil {
			svc.filesAPIClient = files.NewAPIClient(svc.config.FilesAPIURL, svc.config.ServiceAuthToken)
			log.Info(ctx, "files API client created", log.Data{"url": svc.config.FilesAPIURL})
		}
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

	if svc.config.OtelEnabled {
		r.Use(otelmux.Middleware(svc.config.OTServiceName))
		svc.server = svc.serviceList.GetHTTPServer(svc.config.BindAddr, m.Then(otelhttp.NewHandler(r, "/")))
	} else {
		svc.server = svc.serviceList.GetHTTPServer(svc.config.BindAddr, m.Then(r))
	}

	// Create Dataset API
	urlBuilder, err := createURLBuilder(svc.config)
	if err != nil {
		log.Error(ctx, "failed to create URL builder", err)
		return err
	}

	enableURLRewriting := svc.config.EnableURLRewriting
	if enableURLRewriting {
		log.Info(ctx, "URL rewriting enabled")
	}

	datasetPermissions, permissions := getAuthorisationHandlers(ctx, svc.config)
	sm := GetStateMachine(ctx, ds)
	svc.smDS = application.Setup(ds, smDownloadGenerators, sm)
	svc.api = api.Setup(ctx, svc.config, r, ds, urlBuilder, downloadGenerators, datasetPermissions, permissions, enableURLRewriting, svc.smDS)

	// Set the files API client on the DatasetAPI after initialisation
	if svc.config.EnablePrivateEndpoints && svc.filesAPIClient != nil {
		svc.api.SetFilesAPIClient(svc.filesAPIClient, svc.config.ServiceAuthToken)
		log.Info(ctx, "files API client set on dataset API")
	}

	svc.healthCheck.Start(ctx)

	// Log kafka producer errors in parallel go-routine
	if svc.config.EnablePrivateEndpoints {
		svc.generateCMDDownloadsProducer.LogErrors(ctx)
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

func (svc *Service) initMongoDB(ctx context.Context) error {
	var err error
	svc.mongoDB, err = svc.serviceList.GetMongoDB(ctx, svc.config.MongoConfig)
	if err != nil {
		log.Error(ctx, "could not obtain mongo session", err)
	}
	return err
}

func (svc *Service) initGraphDB(ctx context.Context) error {
	var err error
	if !svc.config.EnablePrivateEndpoints || svc.config.DisableGraphDBDependency {
		log.Info(ctx, "skipping graph DB client creation, because it is not required by the enabled endpoints", log.Data{
			"EnablePrivateEndpoints": svc.config.EnablePrivateEndpoints,
		})
		svc.graphDB = &storetest.GraphDBMock{
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
		}
	} else {
		svc.graphDB, svc.graphDBErrorConsumer, err = svc.serviceList.GetGraphDB(ctx)
		if err != nil {
			log.Fatal(ctx, "failed to initialise graph driver", err)
		}
	}
	return err
}

func createURLBuilder(config *config.Configuration) (*url.Builder, error) {
	websiteURL, err := neturl.Parse(config.WebsiteURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse websiteURL from config")
	}

	downloadServiceURL, err := neturl.Parse(config.DownloadServiceURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse downloadServiceURL from config")
	}

	datasetAPIURL, err := neturl.Parse(config.DatasetAPIURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse datasetAPIURL from config")
	}

	codeListAPIURL, err := neturl.Parse(config.CodeListAPIURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse codeListAPIURL from config")
	}

	importAPIURL, err := neturl.Parse(config.ImportAPIURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse importAPIURL from config")
	}

	return url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL), nil
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

		// Close GenerateDownloadsProducer (if it exists)
		if svc.serviceList.GenerateDownloadsProducer {
			log.Info(shutdownContext, "closing generated downloads kafka producer", log.Data{"producer": "DimensionExtracted"})
			if err := svc.generateCMDDownloadsProducer.Close(shutdownContext); err != nil {
				log.Warn(shutdownContext, "error while closing generated downloads kafka producer", log.Data{"producer": "DimensionExtracted", "err": err.Error()})
			}
			log.Info(shutdownContext, "closed generated downloads kafka producer", log.Data{"producer": "DimensionExtracted"})
		}

		// Close GraphDB (if it exists)
		if svc.serviceList.Graph {
			if err := svc.graphDB.Close(shutdownContext); err != nil {
				log.Error(shutdownContext, "failed to close graph db", err)
				hasShutdownError = true
			}

			if err := svc.graphDBErrorConsumer.Close(shutdownContext); err != nil {
				log.Error(shutdownContext, "failed to close graph db error consumer", err)
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

		if err = svc.healthCheck.AddCheck("Kafka Generate Downloads Producer", svc.generateCMDDownloadsProducer.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for CMD kafka downloads producer", err)
		}

		if err = svc.healthCheck.AddCheck("Kafka Generate Cantabular Downloads Producer", svc.generateCantabularDownloadsProducer.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for cantabular kafka downloads producer", err)
		}

		// If running Catabular Locally then don't do health checks against GraphDB
		if !svc.config.DisableGraphDBDependency {
			log.Info(ctx, "private endpoints enabled: adding graph db health check")
			if err = svc.healthCheck.AddCheck("Graph DB", svc.graphDB.Checker); err != nil {
				hasErrors = true
				log.Error(ctx, "error adding check for graph db", err)
			}
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
