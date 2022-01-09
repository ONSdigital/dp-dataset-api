package steps

import (
	"context"
	"fmt"
	"github.com/ONSdigital/dp-dataset-api/models"
	"net/http"
	"strings"

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
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"github.com/ONSdigital/log.go/v2/log"
)

type DatasetComponent struct {
	ErrorFeature        componenttest.ErrorFeature
	svc                 *service.Service
	errorChan           chan error
	MongoClient         *mongo.Mongo
	Config              *config.Configuration
	HTTPServer          *http.Server
	ServiceRunning      bool
	consumer            kafka.IConsumerGroup
	producer            kafka.IProducer
	initialiser         service.Initialiser
	APIFeature          *componenttest.APIFeature
	fakeCantabularBlobs []models.PopulationType
}

func NewDatasetComponent(mongoURI string, zebedeeURL string) (*DatasetComponent, error) {
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
		MongoConfig: config.MongoConfig{
			MongoConnectionConfig: mongodriver.MongoConnectionConfig{
				// TODO the following line can be used as 'normal', i.e. mongoFeature.Server.URI(),
				// when the dp-mongodb has a proper uri parser in place (it's in the pipeline)
				ClusterEndpoint:         strings.Replace(mongoURI, "mongodb://", "", 1),
				Database:                utils.RandomDatabase(),
				Collection:              "datasets",
				ConnectTimeoutInSeconds: c.Config.ConnectTimeoutInSeconds,
				QueryTimeoutInSeconds:   c.Config.QueryTimeoutInSeconds,
			},
			DatasetAPIURL:  "datasets",
			CodeListAPIURL: "",
		}}

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
	// Closing Kafka
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
	// Closing Mongo DB
	if c.svc != nil && c.ServiceRunning {
		if err := c.MongoClient.Connection.DropDatabase(ctx); err != nil {
			log.Warn(ctx, "error dropping database on Close", log.Data{"err": err.Error()})
		}
		if err := c.svc.Close(ctx); err != nil {
			return err
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

func funcClose(_ context.Context) error {
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

func (c *DatasetComponent) DoGetHealthcheckOk(_ *config.Configuration, _ string, _ string, _ string) (service.HealthChecker, error) {
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

func (c *DatasetComponent) DoGetMockedKafkaProducerOk(_ context.Context, _ *config.Configuration, _ string) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}

func (c *DatasetComponent) DoGetMongoDB(_ context.Context, _ config.MongoConfig) (store.MongoDB, error) {
	return c.MongoClient, nil
}

func (c *DatasetComponent) DoGetGraphDBOk(_ context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
			CloseFunc: funcClose,
		},
		&serviceMock.CloserMock{
			CloseFunc: funcClose,
		},
		nil
}

func (c *DatasetComponent) DoGetFakeCantabularOk(ctx context.Context, cfg config.CantabularConfig) store.Cantabular {
	return &storeMock.CantabularMock{
		PopulationTypesFunc: func(ctx context.Context) ([]models.PopulationType, error) {
			return c.fakeCantabularBlobs, nil
		},
	}
}

func (c *DatasetComponent) setInitialiserMock() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       c.DoGetMongoDB,
		DoGetGraphDBFunc:       c.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: c.DoGetMockedKafkaProducerOk,
		DoGetHealthCheckFunc:   c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    c.DoGetHTTPServer,
		DoGetCantabularFunc:    c.DoGetFakeCantabularOk,
	}
}
func (c *DatasetComponent) setInitialiserRealKafka() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       c.DoGetMongoDB,
		DoGetGraphDBFunc:       c.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: c.DoGetKafkaProducer,
		DoGetHealthCheckFunc:   c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    c.DoGetHTTPServer,
		DoGetCantabularFunc:    c.DoGetFakeCantabularOk,
	}
}
