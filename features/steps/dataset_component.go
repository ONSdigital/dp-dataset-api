package steps

import (
	"context"
	"fmt"
	"net/http"

	componenttest "github.com/ONSdigital/dp-component-test"
	"github.com/ONSdigital/dp-component-test/utils"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/service"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/v2/log"
)

type DatasetComponent struct {
	ErrorFeature   componenttest.ErrorFeature
	svc            *service.Service
	errorChan      chan error
	MongoClient    *mongo.Mongo
	Config         *config.Configuration
	HTTPServer     *http.Server
	ServiceRunning bool
	consumer       kafka.IConsumerGroup
	producer       kafka.IProducer
	initialiser    service.Initialiser
}

func NewDatasetComponent(mongoFeature *componenttest.MongoFeature, zebedeeURL string) (*DatasetComponent, error) {
	c := &DatasetComponent{
		HTTPServer:     &http.Server{},
		errorChan:      make(chan error),
		ServiceRunning: false,
	}

	var err error

	c.Config, err = config.Get()
	if err != nil {
		return nil, err
	}

	log.Info(context.Background(), "configuration for component test", log.Data{"config": c.Config})

	c.Config.ZebedeeURL = zebedeeURL

	c.Config.EnablePermissionsAuth = false

	mongodb := &mongo.Mongo{
		CodeListURL: "",
		Collection:  "datasets",
		Database:    utils.RandomDatabase(),
		DatasetURL:  "datasets",
		URI:         mongoFeature.Server.URI(),
	}

	if err := mongodb.Init(context.Background()); err != nil {
		return nil, err
	}

	c.MongoClient = mongodb

	return c, nil
}

func (c *DatasetComponent) Reset() error {
	ctx := context.Background()

	c.MongoClient.Database = utils.RandomDatabase()
	if err := c.MongoClient.Init(ctx); err != nil {
		log.Warn(ctx, "error initialising MongoClient during Reset", log.Data{"err": err.Error()})
	}

	c.Config.EnablePrivateEndpoints = false
	// Resets back to Mocked Kafka
	c.setInitialiserMock()

	return nil
}

func (c *DatasetComponent) Close() error {
	ctx := context.Background()
	if c.consumer != nil {
		if err := c.consumer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close Kafka consumer %w", err)
		}
	}
	if c.producer != nil {
		if err := c.producer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close Kafka producer %w", err)
		}
	}

	if c.svc != nil && c.ServiceRunning {
		if err := c.svc.Close(ctx); err != nil {
			return fmt.Errorf("failed to close service: %w", err)
		}
		c.ServiceRunning = false
	}
	return nil
}

func (c *DatasetComponent) InitialiseService() (http.Handler, error) {
	// Initialiser before Run to allow switching out of Initialiser between tests.
	c.svc = service.New(c.Config, service.NewServiceList(c.initialiser))

	if err := c.svc.Run(context.Background(), "1", "", "", c.errorChan); err != nil {
		return nil, err
	}
	c.ServiceRunning = true
	return c.HTTPServer.Handler, nil
}

func funcClose(ctx context.Context) error {
	return nil
}

func (c *DatasetComponent) setConsumer(topic string) error {
	var err error
	kafkaOffset := kafka.OffsetOldest
	if c.consumer, err = kafka.NewConsumerGroup(
		context.Background(),
		c.Config.KafkaAddr,
		topic,
		"test-kafka-group",
		kafka.CreateConsumerGroupChannels(1),
		&kafka.ConsumerGroupConfig{
			Offset:       &kafkaOffset,
			KafkaVersion: &c.Config.KafkaVersion,
		},
	); err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}
	return nil
}

func (c *DatasetComponent) DoGetHealthcheckOk(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
	return &serviceMock.HealthCheckerMock{
		AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
		StartFunc:    func(ctx context.Context) {},
		StopFunc:     func() {},
	}, nil
}

func (c *DatasetComponent) DoGetHTTPServer(bindAddr string, router http.Handler) service.HTTPServer {
	c.HTTPServer.Addr = bindAddr
	c.HTTPServer.Handler = router
	return c.HTTPServer
}

// DoGetMongoDB returns a MongoDB
func (c *DatasetComponent) DoGetMongoDB(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {
	return c.MongoClient, nil
}

func (c *DatasetComponent) DoGetGraphDBOk(ctx context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{
			CloseFunc:                  funcClose,
			SetInstanceIsPublishedFunc: func(ctx context.Context, instanceID string) error { return nil },
		},
		&serviceMock.CloserMock{CloseFunc: funcClose}, nil
}

func (c *DatasetComponent) DoGetKafkaProducer(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error) {
	pConfig := &kafka.ProducerConfig{
		KafkaVersion: &cfg.KafkaVersion,
	}

	if cfg.KafkaSecProtocol == "TLS" {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}

	pChannels := kafka.CreateProducerChannels()
	return kafka.NewProducer(ctx, cfg.KafkaAddr, topic, pChannels, pConfig)
}

func (c *DatasetComponent) DoGetMockedKafkaProducerOk(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}

func (c *DatasetComponent) setInitialiserMock() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       c.DoGetMongoDB,
		DoGetGraphDBFunc:       c.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: c.DoGetMockedKafkaProducerOk,
		DoGetHealthCheckFunc:   c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    c.DoGetHTTPServer,
	}
}
func (c *DatasetComponent) setInitialiserRealKafka() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       c.DoGetMongoDB,
		DoGetGraphDBFunc:       c.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: c.DoGetKafkaProducer,
		DoGetHealthCheckFunc:   c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    c.DoGetHTTPServer,
	}
}
