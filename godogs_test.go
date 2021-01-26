package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

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
	"github.com/cucumber/godog"
)

type TestContext struct {
	response *http.Response
}

type apiFeature struct {
	err error
	svc *service.Service
	// context   TestContext
	errorChan    chan error
	httpServer   *http.Server
	httpResponse *http.Response
}

func (f *apiFeature) Errorf(format string, args ...interface{}) {
	f.err = fmt.Errorf(format, args...)
}

func (f *apiFeature) iGET(path string) error {
	req := httptest.NewRequest("GET", "http://"+f.httpServer.Addr+path, nil)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.httpServer.Handler.ServeHTTP(w, req)

	f.httpResponse = w.Result()

	fmt.Println("response: ", f.httpResponse)
	return nil
}

// func (f *apiFeature) jonTriesToBorrowTheBook(memberName string, bookName string) error {

// 	payload := map[string]string{
// 		"member_name": memberName,
// 		"book_name":   bookName,
// 	}

// 	data, err := json.Marshal(payload)
// 	if err != nil {
// 		return err
// 	}

// 	req := httptest.NewRequest("PUT", "http://example.com/dataset", bytes.NewReader(data))
// 	req.Header.Set("Content-Type", "application/json")
// 	w := httptest.NewRecorder()

// 	// f.app.Router.ServeHTTP(w, req)

// 	f.context.response = w.Result()

// 	return f.err
// }

func InitializeScenario(ctx *godog.ScenarioContext) {
	var f *apiFeature = &apiFeature{}

	f = &apiFeature{
		errorChan:  make(chan error),
		httpServer: &http.Server{},
	}

	ctx.BeforeScenario(func(*godog.Scenario) {

		cfg, err := config.Get()
		if err != nil {
			panic(err)
		}

		var funcClose = func(ctx context.Context) error {
			return nil
		}
		hcMock := &mock.HealthCheckerMock{
			AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
			StartFunc:    func(ctx context.Context) {},
			StopFunc:     func() {},
		}
		funcDoGetHealthcheckOk := func(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}
		funcDoGetHTTPServer := func(bindAddr string, router http.Handler) service.HTTPServer {
			f.httpServer.Addr = bindAddr
			f.httpServer.Handler = router
			return f.httpServer
		}
		funcDoGetMongoDBOk := func(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {
			return &storeMock.MongoDBMock{
				CloseFunc: funcClose,
				GetDatasetsFunc: func(context.Context) ([]models.DatasetUpdate, error) {
					return []models.DatasetUpdate{}, nil
				},
			}, nil
		}
		funcDoGetGraphDBOk := func(ctx context.Context) (store.GraphDB, service.Closer, error) {
			return &storeMock.GraphDBMock{CloseFunc: funcClose}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
		}
		funcDoGetKafkaProducerOk := func(ctx context.Context, cfg *config.Configuration) (kafka.IProducer, error) {
			return &kafkatest.IProducerMock{
				ChannelsFunc: func() *kafka.ProducerChannels {
					return &kafka.ProducerChannels{}
				},
				CloseFunc: funcClose,
			}, nil
		}

		initMock := &serviceMock.InitialiserMock{
			DoGetMongoDBFunc:       funcDoGetMongoDBOk,
			DoGetGraphDBFunc:       funcDoGetGraphDBOk,
			DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
			DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
			DoGetHTTPServerFunc:    funcDoGetHTTPServer,
		}

		f.svc = service.New(cfg, service.NewServiceList(initMock))
		if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
			panic(err)
		}

	})

	ctx.AfterScenario(func(*godog.Scenario, error) {
		if f != nil && f.svc != nil {
			f.svc.Close(context.Background())
		}
	})

	ctx.Step(`^I GET "([^"]*)"$`, f.iGET)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {

	})
}

func TestMain(m *testing.M) {
	flag.Parse()
	opts.Paths = flag.Args()

	status := godog.TestSuite{
		Name:                 "godogs",
		TestSuiteInitializer: InitializeTestSuite,
		ScenarioInitializer:  InitializeScenario,
		Options:              &opts,
	}.Run()

	// Optional: Run `testing` package's logic besides godogt.
	if st := m.Run(); st > status {
		status = st
	}

	os.Exit(status)
}
