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
	Mongo        *memongo.Server
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
	f.Mongo = mongoServer

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

	return f
}

func (f *APIFeature) Close() error {
	if f.svc != nil {
		f.svc.Close(context.Background())
	}
	f.Mongo.Stop()
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
		CodeListURL: cfg.CodeListAPIURL,
		Collection:  cfg.MongoConfig.Collection,
		Database:    cfg.MongoConfig.Database,
		DatasetURL:  cfg.DatasetAPIURL,
		URI:         cfg.MongoConfig.BindAddr,
	}
	if err := mongodb.Init(); err != nil {
		return nil, err
	}
	log.Event(ctx, "listening to mongo db session", log.INFO, log.Data{"URI": mongodb.URI})
	return mongodb, nil
}

// func (f *APIFeature) DoGetMongoDBOk(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {

// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer mongoServer.Stop()

// 	return &storeMock.MongoDBMock{
// 		CloseFunc: funcClose,
// 		GetDatasetsFunc: func(context.Context) ([]models.DatasetUpdate, error) {
// 			response := make([]models.DatasetUpdate, 0)
// 			for _, dataset := range f.Datasets {
// 				response = append(response, models.DatasetUpdate{
// 					ID:      dataset.ID,
// 					Current: dataset,
// 					Next:    dataset,
// 				})
// 			}
// 			return response, nil
// 		},
// 		GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
// 			response := models.DatasetUpdate{
// 				ID:      ID,
// 				Current: &models.Dataset{},
// 			}
// 			return &response, nil
// 		},
// 	}, nil
// }

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
