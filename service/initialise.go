package service

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
)

// ExternalServiceList holds the initialiser and initialisation state of external services.
type ExternalServiceList struct {
	GenerateDownloadsProducer bool
	Graph                     bool
	HealthCheck               bool
	MongoDB                   bool
	Init                      Initialiser
}

// NewServiceList creates a new service list with the provided initialiser
func NewServiceList(initialiser Initialiser) *ExternalServiceList {
	return &ExternalServiceList{
		Init: initialiser,
	}
}

// Init implements the Initialiser interface to initialise dependencies
type Init struct{}

// GetHTTPServer creates an http server
func (e *ExternalServiceList) GetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := e.Init.DoGetHTTPServer(bindAddr, router)
	return s
}

// GetHealthCheck creates a healthcheck with versionInfo and sets the HealthCheck flag to true
func (e *ExternalServiceList) GetHealthCheck(cfg *config.Configuration, buildTime, gitCommit, version string) (HealthChecker, error) {
	hc, err := e.Init.DoGetHealthCheck(cfg, buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	e.HealthCheck = true
	return hc, nil
}

// GetProducer returns a kafka producer, which might not be initialised yet.
func (e *ExternalServiceList) GetProducer(ctx context.Context, cfg *config.Configuration, topic string) (kafkaProducer kafka.IProducer, err error) {
	kafkaProducer, err = e.Init.DoGetKafkaProducer(ctx, cfg, topic)
	if err != nil {
		return
	}
	e.GenerateDownloadsProducer = true
	return
}

// GetGraphDB returns a graphDB (only if observation and private endpoint are enabled)
func (e *ExternalServiceList) GetGraphDB(ctx context.Context) (store.GraphDB, Closer, error) {
	graphDB, graphDBErrorConsumer, err := e.Init.DoGetGraphDB(ctx)
	if err != nil {
		return nil, nil, err
	}
	e.Graph = true
	return graphDB, graphDBErrorConsumer, nil
}

// GetMongoDB returns a mongodb health client and dataset mongo object
func (e *ExternalServiceList) GetMongoDB(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error) {
	mongodb, err := e.Init.DoGetMongoDB(ctx, cfg)
	if err != nil {
		log.Error(ctx, "failed to initialise mongo", err)
		return nil, err
	}
	e.MongoDB = true
	return mongodb, nil
}

// DoGetHTTPServer creates an HTTP Server with the provided bind address and router
func (e *Init) DoGetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// DoGetHealthCheck creates a healthcheck with versionInfo
func (e *Init) DoGetHealthCheck(cfg *config.Configuration, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	return &hc, nil
}

// DoGetKafkaProducer creates a new Kafka Producer
func (e *Init) DoGetKafkaProducer(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error) {

	pConfig := &kafka.ProducerConfig{
		KafkaVersion:      &cfg.KafkaVersion,
		Topic:             topic,
		BrokerAddrs:       cfg.KafkaAddr,
		MinBrokersHealthy: &cfg.KafkaProducerMinBrokersHealthy,
	}

	if cfg.KafkaSecProtocol == "TLS" {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}

	return kafka.NewProducer(ctx, pConfig)
}

// DoGetGraphDB creates a new GraphDB
func (e *Init) DoGetGraphDB(ctx context.Context) (store.GraphDB, Closer, error) {
	graphDB, err := graph.New(ctx, graph.Subsets{Observation: true, Instance: true})
	if err != nil {
		return nil, nil, err
	}

	graphDBErrorConsumer := graph.NewLoggingErrorConsumer(ctx, graphDB.ErrorChan())

	return graphDB, graphDBErrorConsumer, nil
}

// DoGetMongoDB returns a MongoDB
func (e *Init) DoGetMongoDB(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error) {
	mongodb := &mongo.Mongo{
		MongoConfig: cfg,
	}
	if err := mongodb.Init(ctx); err != nil {
		return nil, err
	}
	log.Info(ctx, "listening to mongo db session", log.Data{"URI": mongodb.ClusterEndpoint})
	return mongodb, nil
}
