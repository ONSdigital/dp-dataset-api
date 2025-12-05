package steps

import (
	"context"
	"fmt"
	"net/http"
	"time"

	permissionsSDK "github.com/ONSdigital/dp-permissions-api/sdk"

	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	"github.com/ONSdigital/dp-authorisation/v2/authorisationtest"
	componenttest "github.com/ONSdigital/dp-component-test"
	"github.com/ONSdigital/dp-component-test/utils"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/service"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	filesAPISDK "github.com/ONSdigital/dp-files-api/sdk"
	filesAPISDKMocks "github.com/ONSdigital/dp-files-api/sdk/mocks"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/kafkatest"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"github.com/ONSdigital/log.go/v2/log"
)

type DatasetComponent struct {
	ErrorFeature            componenttest.ErrorFeature
	apiFeature              *componenttest.APIFeature
	svc                     *service.Service
	errorChan               chan error
	MongoClient             *mongo.Mongo
	Config                  *config.Configuration
	HTTPServer              *http.Server
	ServiceRunning          bool
	consumer                kafka.IConsumerGroup
	producer                kafka.IProducer
	initialiser             service.Initialiser
	AuthorisationMiddleware authorisation.Middleware
}

func NewDatasetComponent(mongoURI, zebedeeURL string) (*DatasetComponent, error) {
	c := &DatasetComponent{
		HTTPServer: &http.Server{
			ReadHeaderTimeout: 60 * time.Second,
		},
		errorChan:      make(chan error),
		ServiceRunning: false,
	}

	var err error

	c.Config, err = config.Get()
	if err != nil {
		return nil, err
	}

	log.Info(context.Background(), "configuration for component test", log.Data{"config": c.Config})

	fakePermissionsAPI := setupFakePermissionsAPI()
	c.Config.AuthConfig.PermissionsAPIURL = fakePermissionsAPI.URL()

	c.Config.ZebedeeURL = zebedeeURL

	mongodb := &mongo.Mongo{
		MongoConfig: config.MongoConfig{
			MongoDriverConfig: mongodriver.MongoDriverConfig{
				ClusterEndpoint: mongoURI,
				Database:        utils.RandomDatabase(),
				Collections:     c.Config.Collections,
				ConnectTimeout:  c.Config.ConnectTimeout,
				QueryTimeout:    c.Config.QueryTimeout,
			},
			DatasetAPIURL:  "datasets",
			CodeListAPIURL: "",
		}}

	if err := mongodb.Init(context.Background()); err != nil {
		return nil, err
	}

	c.MongoClient = mongodb
	c.apiFeature = componenttest.NewAPIFeature(c.InitialiseService)

	return c, nil
}

func (c *DatasetComponent) Reset() error {
	ctx := context.Background()

	c.MongoClient.Database = utils.RandomDatabase()
	if err := c.MongoClient.Init(ctx); err != nil {
		log.Warn(ctx, "error initialising MongoClient during Reset", log.Data{"err": err.Error()})
	}

	c.Config.EnablePrivateEndpoints = false
	c.Config.EnableURLRewriting = false
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

func funcClose(context.Context) error {
	return nil
}

func (c *DatasetComponent) setConsumer(topic string) error {
	ctx := context.Background()

	var err error
	kafkaOffset := kafka.OffsetOldest
	if c.consumer, err = kafka.NewConsumerGroup(
		ctx,
		&kafka.ConsumerGroupConfig{
			BrokerAddrs:       c.Config.KafkaAddr,
			Topic:             topic,
			Offset:            &kafkaOffset,
			KafkaVersion:      &c.Config.KafkaVersion,
			GroupName:         "test-kafka-group",
			MinBrokersHealthy: &c.Config.KafkaConsumerMinBrokersHealthy,
			OtelEnabled:       &c.Config.OtelEnabled,
		},
	); err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}

	// start consumer group
	if err := c.consumer.Start(); err != nil {
		return fmt.Errorf("error starting kafka consumer: %w", err)
	}

	c.consumer.LogErrors(ctx)

	c.consumer.StateWait(kafka.Consuming)
	log.Info(context.Background(), "component-test kafka consumer is in consuming state")

	return nil
}

func (c *DatasetComponent) DoGetHealthcheckOk(*config.Configuration, string, string, string) (service.HealthChecker, error) {
	return &serviceMock.HealthCheckerMock{
		AddCheckFunc: func(string, healthcheck.Checker) error { return nil },
		StartFunc:    func(context.Context) {},
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

func (c *DatasetComponent) DoGetMockedKafkaProducerOk(context.Context, *config.Configuration, string) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
		LogErrorsFunc: func(context.Context) {
			// Do nothing
		},
	}, nil
}

func (c *DatasetComponent) DoGetMongoDB(context.Context, config.MongoConfig) (store.MongoDB, error) {
	return c.MongoClient, nil
}

func (c *DatasetComponent) DoGetGraphDBOk(context.Context) (store.GraphDB, service.Closer, error) {
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

func (c *DatasetComponent) DoGetAuthorisationMiddleware(ctx context.Context, cfg *authorisation.Config) (authorisation.Middleware, error) {
	middleware, err := authorisation.NewMiddlewareFromConfig(ctx, cfg, cfg.JWTVerificationPublicKeys)
	if err != nil {
		return nil, err
	}

	c.AuthorisationMiddleware = middleware
	return c.AuthorisationMiddleware, nil
}

func (c *DatasetComponent) DoGetFilesAPIClientOk(ctx context.Context, cfg *config.Configuration) (filesAPISDK.Clienter, error) {
	return &filesAPISDKMocks.ClienterMock{
		DeleteFileFunc: func(ctx context.Context, filePath string) error {
			if filePath == "/fail/to/delete.csv" {
				return fmt.Errorf("failed to delete file at path: %s", filePath)
			}
			return nil
		},
	}, nil
}

func (c *DatasetComponent) setInitialiserMock() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:                 c.DoGetMongoDB,
		DoGetGraphDBFunc:                 c.DoGetGraphDBOk,
		DoGetFilesAPIClientFunc:          c.DoGetFilesAPIClientOk,
		DoGetKafkaProducerFunc:           c.DoGetMockedKafkaProducerOk,
		DoGetHealthCheckFunc:             c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:              c.DoGetHTTPServer,
		DoGetAuthorisationMiddlewareFunc: c.DoGetAuthorisationMiddleware,
	}
}
func (c *DatasetComponent) setInitialiserRealKafka() {
	c.initialiser = &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:                 c.DoGetMongoDB,
		DoGetGraphDBFunc:                 c.DoGetGraphDBOk,
		DoGetFilesAPIClientFunc:          c.DoGetFilesAPIClientOk,
		DoGetKafkaProducerFunc:           c.DoGetKafkaProducer,
		DoGetHealthCheckFunc:             c.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:              c.DoGetHTTPServer,
		DoGetAuthorisationMiddlewareFunc: c.DoGetAuthorisationMiddleware,
	}
}

func setupFakePermissionsAPI() *authorisationtest.FakePermissionsAPI {
	fakePermissionsAPI := authorisationtest.NewFakePermissionsAPI()
	dataset := getPermissionsDataset()
	fakePermissionsAPI.Reset()
	if err := fakePermissionsAPI.UpdatePermissionsBundleResponse(dataset); err != nil {
		log.Error(context.Background(), "failed to update permissions bundle response", err)
	}
	return fakePermissionsAPI
}

func getPermissionsDataset() *permissionsSDK.Bundle {
	return &permissionsSDK.Bundle{
		"datasets:read": { // role
			"groups/role-admin": { // group
				{
					ID: "1", // policy
				},
			},
		},
		"datasets:create": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"datasets:update": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"datasets:delete": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-editions-versions:delete": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-editions-versions:create": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-editions-versions:read": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-editions-versions:update": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-instances:create": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-instances:read": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
		"dataset-instances:update": {
			"groups/role-admin": {
				{
					ID: "1",
				},
			},
		},
	}
}
