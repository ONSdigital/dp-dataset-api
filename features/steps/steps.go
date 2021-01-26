package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"

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
	"github.com/stretchr/testify/assert"
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
	Datasets     []*models.Dataset
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

	return nil
}

func (f *apiFeature) iHaveTheseDatasets(datasets *godog.DocString) error {

	err := json.Unmarshal([]byte(datasets.Content), &f.Datasets)
	if err != nil {
		return err
	}

	return nil
}

func (f *apiFeature) iShouldReceiveTheFollowingJSONResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.JSONEq(f, string(body), expectedAPIResponse.Content)

	return f.err
}

func (f *apiFeature) theHTTPStatusCodeShouldBe(expectedCodeStr string) error {
	expectedCode, err := strconv.Atoi(expectedCodeStr)
	if err != nil {
		return err
	}
	assert.Equal(f, expectedCode, f.httpResponse.StatusCode)
	return f.err
}

func (f *apiFeature) theResponseHeaderShouldBe(headerName, expectedValue string) error {
	assert.Equal(f, expectedValue, f.httpResponse.Header.Get(headerName))
	return f.err
}

func (f *apiFeature) iShouldReceiveTheFollowingJSONResponseWithStatus(expectedCodeStr string, expectedBody *godog.DocString) error {
	if err := f.theHTTPStatusCodeShouldBe(expectedCodeStr); err != nil {
		return err
	}
	if err := f.theResponseHeaderShouldBe("Content-Type", "application/json"); err != nil {
		return err
	}
	return f.iShouldReceiveTheFollowingJSONResponse(expectedBody)
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	fmt.Println("JOE IS NOT A DOG")

	var f *apiFeature = &apiFeature{
		errorChan:  make(chan error),
		httpServer: &http.Server{},
		Datasets:   make([]*models.Dataset, 0),
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
	ctx.Step(`^I should receive the following JSON response:$`, f.iShouldReceiveTheFollowingJSONResponse)
	ctx.Step(`^the HTTP status code should be "([^"]*)"$`, f.theHTTPStatusCodeShouldBe)
	ctx.Step(`^the response header "([^"]*)" should be "([^"]*)"$`, f.theResponseHeaderShouldBe)
	ctx.Step(`^I should receive the following JSON response with status "([^"]*)":$`, f.iShouldReceiveTheFollowingJSONResponseWithStatus)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
	})
}
