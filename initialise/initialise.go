package initialise

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	mongoHealth "github.com/ONSdigital/dp-mongodb/health"
	"github.com/ONSdigital/log.go/log"
)

// ExternalServiceList represents a list of services
type ExternalServiceList struct {
	AuditProducer             bool
	GenerateDownloadsProducer bool
	Graph                     bool
	HealthCheck               bool
	MongoDB                   bool
}

// KafkaProducerName : Type for kafka producer name used by iota constants
type KafkaProducerName int

// Possible names of Kafa Producers
const (
	Audit = iota
	GenerateDownloads
)

var kafkaProducerNames = []string{"Audit", "GenerateDownloads"}

// Values of the kafka producers names
func (k KafkaProducerName) String() string {
	return kafkaProducerNames[k]
}

// GetProducer returns a kafka producer, which might not be initialised yet.
func (e *ExternalServiceList) GetProducer(ctx context.Context, kafkaBrokers []string, topic string, name KafkaProducerName, envMax int) (kafkaProducer *kafka.Producer, err error) {
	pChannels := kafka.CreateProducerChannels()
	kafkaProducer, err = kafka.NewProducer(ctx, kafkaBrokers, topic, envMax, pChannels)
	if err != nil {
		return
	}

	switch {
	case name == Audit:
		e.AuditProducer = true
	case name == GenerateDownloads:
		e.GenerateDownloadsProducer = true
	default:
		err = fmt.Errorf("Kafka producer name not recognised: '%s'. Valid names: %v", name.String(), kafkaProducerNames)
	}

	return
}

// GetGraphDB returns a graphDB
func (e *ExternalServiceList) GetGraphDB(ctx context.Context, enableObservationEndpoint bool) (*graph.DB, error) {

	// the graph DB is only used for the observation endpoint
	if !enableObservationEndpoint {
		return nil, nil
	}

	graphDB, err := graph.New(ctx, graph.Subsets{Observation: true, Instance: true})
	if err != nil {
		return nil, err
	}

	e.Graph = true

	return graphDB, nil
}

// GetMongoDB returns a mongodb client and dataset mongo object
func (e *ExternalServiceList) GetMongoDB(ctx context.Context, cfg *config.Configuration) (*mongoHealth.Client, *mongo.Mongo, error) {
	mongodb := &mongo.Mongo{
		CodeListURL: cfg.CodeListAPIURL,
		Collection:  cfg.MongoConfig.Collection,
		Database:    cfg.MongoConfig.Database,
		DatasetURL:  cfg.DatasetAPIURL,
		URI:         cfg.MongoConfig.BindAddr,
	}

	session, err := mongodb.Init()
	if err != nil {
		log.Event(ctx, "failed to initialise mongo", log.ERROR, log.Error(err))
		return nil, nil, err
	} else {
		mongodb.Session = session
		log.Event(ctx, "listening to mongo db session", log.INFO, log.Data{
			"bind_address": cfg.BindAddr,
		})
	}

	client := mongoHealth.NewClient(session)

	e.MongoDB = true

	return client, mongodb, nil
}

// GetHealthCheck creates a healthcheck with versionInfo
func (e *ExternalServiceList) GetHealthCheck(cfg *config.Configuration, buildTime, gitCommit, version string) (healthcheck.HealthCheck, error) {

	// Create healthcheck object with versionInfo
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return healthcheck.HealthCheck{}, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)

	e.HealthCheck = true

	return hc, nil
}
