package steps

import (
	"context"
	"net/http"

	componenttest "github.com/ONSdigital/dp-component-test"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/service"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/benweissmann/memongo"
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

func NewDatasetComponent(mongoFeature *componenttest.MongoFeature, zebedeeURL string) (*DatasetComponent, error) {

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
		CodeListURL: "",
		Collection:  "datasets",
		Database:    memongo.RandomDatabase(),
		DatasetURL:  "datasets",
		URI:         mongoFeature.Server.URI(),
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
	f.MongoClient.Database = memongo.RandomDatabase()
	f.MongoClient.Init(context.Background())
	f.Config.EnablePrivateEndpoints = false
	return f
}

func (f *DatasetComponent) Close() error {
	if f.svc != nil && f.ServiceRunning {
		f.svc.Close(context.Background())
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

func funcClose(ctx context.Context) error {
	return nil
}

func (f *DatasetComponent) DoGetHealthcheckOk(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
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
func (f *DatasetComponent) DoGetMongoDB(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {
	return f.MongoClient, nil
}

func (f *DatasetComponent) DoGetGraphDBOk(ctx context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{CloseFunc: funcClose}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
}

func (f *DatasetComponent) DoGetKafkaProducerOk(ctx context.Context, cfg *config.Configuration) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}
