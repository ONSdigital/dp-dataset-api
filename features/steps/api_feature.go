package steps

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/service"
	"github.com/ONSdigital/dp-dataset-api/service/mock"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
)

type APIFeature struct {
	err error
	svc *service.Service
	// context   TestContext
	errorChan    chan error
	httpServer   *http.Server
	httpResponse *http.Response
	Datasets     []*models.Dataset
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

	initMock := &serviceMock.InitialiserMock{
		DoGetMongoDBFunc:       f.DoGetMongoDBOk,
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

func (f *APIFeature) DoGetMongoDBOk(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {
	return &storeMock.MongoDBMock{
		CloseFunc: funcClose,
		GetDatasetsFunc: func(context.Context) ([]models.DatasetUpdate, error) {
			response := make([]models.DatasetUpdate, 0)
			for _, dataset := range f.Datasets {
				response = append(response, models.DatasetUpdate{
					ID:      dataset.ID,
					Current: dataset,
					Next:    dataset,
				})
			}
			return response, nil
		},
		GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
			response := models.DatasetUpdate{
				ID:      ID,
				Current: &models.Dataset{},
			}
			return &response, nil
		},
	}, nil
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
