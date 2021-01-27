package steps

import (
	"context"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/service"
	"github.com/ONSdigital/dp-dataset-api/service/mock"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/log"
	"github.com/benweissmann/memongo"
)

type APIFeature struct {
	err error
	svc *service.Service
	// context   TestContext
	errorChan    chan error
	httpServer   *http.Server
	httpResponse *http.Response
	Datasets     []*models.Dataset
	MongoServer  *memongo.Server
	MongoClient  *mongo.Mongo
}

func NewAPIFeature() *APIFeature {

	f := &APIFeature{
		errorChan:  make(chan error),
		httpServer: &http.Server{},
		Datasets:   make([]*models.Dataset, 0),
	}

	cfg, err := config.Get()
	if err != nil {
		panic(err)
	}
	opts := memongo.Options{
		Port:           27017,
		MongoVersion:   "4.0.5",
		StartupTimeout: time.Second * 10,
	}

	mongoServer, err := memongo.StartWithOptions(&opts)
	if err != nil {
		panic(err)
	}
	f.MongoServer = mongoServer

	initMock := &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       f.DoGetMongoDB,
		DoGetGraphDBFunc:       f.DoGetGraphDBOk,
		DoGetKafkaProducerFunc: f.DoGetKafkaProducerOk,
		DoGetHealthCheckFunc:   f.DoGetHealthcheckOk,
		DoGetHTTPServerFunc:    f.DoGetHTTPServer,
	}

	f.svc = service.New(cfg, service.NewServiceList(initMock))
	return f
}

func (f *APIFeature) Reset() *APIFeature {
	f.Datasets = make([]*models.Dataset, 0)
	if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
		panic(err)
	}

	f.MongoClient.Database = memongo.RandomDatabase()
	f.MongoClient.Init()
	return f
}

func (f *APIFeature) Close() error {
	if f.svc != nil {
		f.svc.Close(context.Background())
	}
	f.MongoServer.Stop()
	return nil
}

func funcClose(ctx context.Context) error {
	return nil
}

func (f *APIFeature) DoGetHealthcheckOk(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
	return &mock.HealthCheckerMock{
		AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
		StartFunc:    func(ctx context.Context) {},
		StopFunc:     func() {},
	}, nil
}

func (f *APIFeature) DoGetHTTPServer(bindAddr string, router http.Handler) service.HTTPServer {
	f.httpServer.Addr = bindAddr
	f.httpServer.Handler = router
	return f.httpServer
}

// DoGetMongoDB returns a MongoDB
func (f *APIFeature) DoGetMongoDB(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {

	mongodb := &mongo.Mongo{
		CodeListURL: "",
		Collection:  "datasets",
		Database:    memongo.RandomDatabase(),
		DatasetURL:  "datasets",
		URI:         f.MongoServer.URI(),
	}
	if err := mongodb.Init(); err != nil {
		return nil, err
	}
	log.Event(ctx, "listening to mongo db session", log.INFO, log.Data{"URI": mongodb.URI})

	f.MongoClient = mongodb

	return mongodb, nil
}

func (f *APIFeature) DoGetGraphDBOk(ctx context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{CloseFunc: funcClose}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
}

func (f *APIFeature) DoGetKafkaProducerOk(ctx context.Context, cfg *config.Configuration) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}
