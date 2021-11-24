package steps

import (
	"context"
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
}

func NewDatasetComponent(mongoURI string, zebedeeURL string) (*DatasetComponent, error) {

	f := &DatasetComponent{
		HTTPServer:     &http.Server{},
		errorChan:      make(chan error),
		ServiceRunning: false,
	}

	var err error

	f.Config, err = config.Get()
	if err != nil {
		return nil, err
	}

	f.Config.ZebedeeURL = zebedeeURL

	f.Config.EnablePermissionsAuth = false

	mongodb := &mongo.Mongo{
		MongoConfig: config.MongoConfig{
			// TODO the following line can be used as 'normal', i.e. mongoFeature.Server.URI(),
			// when the dp-mongodb has a proper uri parser in place (it's in the pipeline)
			URI:               strings.Replace(mongoURI, "mongodb://", "", 1),
			Database:          utils.RandomDatabase(),
			Collection:        "datasets",
			DatasetAPIURL:     "datasets",
			CodeListAPIURL:    "",
			ConnectionTimeout: f.Config.ConnectionTimeout,
			QueryTimeout:      f.Config.QueryTimeout,
		},
	}

	if err := mongodb.Init(context.Background()); err != nil {
		return nil, err
	}

	f.MongoClient = mongodb

	initMock := &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       f.DoGetMongoDB,
		DoGetGraphDBFunc:       f.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: f.DoGetKafkaProducerOk,
		DoGetHealthCheckFunc:   f.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    f.DoGetHTTPServer,
	}

	f.svc = service.New(f.Config, service.NewServiceList(initMock))

	return f, nil
}

func (f *DatasetComponent) Reset() *DatasetComponent {
	ctx := context.Background()
	if err := f.MongoClient.Connection.DropDatabase(ctx); err != nil {
		log.Warn(ctx, "error dropping database on Reset", log.Data{"err": err.Error()})
	}
	f.MongoClient.Database = utils.RandomDatabase()
	if err := f.MongoClient.Init(ctx); err != nil {
		log.Warn(ctx, "error initialising MongoClient during Reset", log.Data{"err": err.Error()})
	}
	f.Config.EnablePrivateEndpoints = false
	return f
}

func (f *DatasetComponent) Close() error {
	ctx := context.Background()
	if f.svc != nil && f.ServiceRunning {
		if err := f.MongoClient.Connection.DropDatabase(ctx); err != nil {
			log.Warn(ctx, "error dropping database on Close", log.Data{"err": err.Error()})
		}
		if err := f.svc.Close(ctx); err != nil {
			return err
		}
		f.ServiceRunning = false
	}
	return nil
}

func (f *DatasetComponent) InitialiseService() (http.Handler, error) {
	if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
		return nil, err
	}
	f.ServiceRunning = true
	return f.HTTPServer.Handler, nil
}

func funcClose(_ context.Context) error {
	return nil
}

func (f *DatasetComponent) DoGetHealthcheckOk(_ *config.Configuration, _ string, _ string, _ string) (service.HealthChecker, error) {
	return &serviceMock.HealthCheckerMock{
		AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
		StartFunc:    func(ctx context.Context) {},
		StopFunc:     func() {},
	}, nil
}

func (f *DatasetComponent) DoGetHTTPServer(bindAddr string, router http.Handler) service.HTTPServer {
	f.HTTPServer.Addr = bindAddr
	f.HTTPServer.Handler = router
	return f.HTTPServer
}

// DoGetMongoDB returns a MongoDB
func (f *DatasetComponent) DoGetMongoDB(_ context.Context, _ config.MongoConfig) (store.MongoDB, error) {
	return f.MongoClient, nil
}

func (f *DatasetComponent) DoGetGraphDBOk(_ context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{CloseFunc: funcClose}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
}

func (f *DatasetComponent) DoGetKafkaProducerOk(_ context.Context, _ *config.Configuration, _ string) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}
