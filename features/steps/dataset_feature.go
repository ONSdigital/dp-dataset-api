package feature

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	componenttest "github.com/ONSdigital/dp-component-test"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/service"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/benweissmann/memongo"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

type DatasetFeature struct {
	ErrorFeature   componenttest.ErrorFeature
	svc            *service.Service
	errorChan      chan error
	Datasets       []*models.Dataset
	MongoClient    *mongo.Mongo
	Config         *config.Configuration
	HTTPServer     *http.Server
	ServiceRunning bool
}

func NewDatasetFeature(mongoFeature *componenttest.MongoFeature, zebedeeURL string) (*DatasetFeature, error) {

	f := &DatasetFeature{
		HTTPServer:     &http.Server{},
		errorChan:      make(chan error),
		Datasets:       make([]*models.Dataset, 0),
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

	if err := mongodb.Init(); err != nil {
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

func (f *DatasetFeature) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^private endpoints are enabled$`, f.privateEndpointsAreEnabled)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, f.theDocumentInTheDatabaseForIdShouldBe)
	ctx.Step(`^there are no datasets$`, f.thereAreNoDatasets)
}

func (f *DatasetFeature) Reset() *DatasetFeature {
	f.Datasets = make([]*models.Dataset, 0)
	f.MongoClient.Database = memongo.RandomDatabase()
	f.MongoClient.Init()
	f.Config.EnablePrivateEndpoints = false
	return f
}

func (f *DatasetFeature) Close() error {
	if f.svc != nil && f.ServiceRunning {
		f.svc.Close(context.Background())
		f.ServiceRunning = false
	}
	return nil
}

func (f *DatasetFeature) InitialiseService() (http.Handler, error) {
	if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
		return nil, err
	}
	f.ServiceRunning = true
	return f.HTTPServer.Handler, nil
}

func funcClose(ctx context.Context) error {
	return nil
}

func (f *DatasetFeature) DoGetHealthcheckOk(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
	return &serviceMock.HealthCheckerMock{
		AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
		StartFunc:    func(ctx context.Context) {},
		StopFunc:     func() {},
	}, nil
}

func (f *DatasetFeature) DoGetHTTPServer(bindAddr string, router http.Handler) service.HTTPServer {
	f.HTTPServer.Addr = bindAddr
	f.HTTPServer.Handler = router
	return f.HTTPServer
}

// DoGetMongoDB returns a MongoDB
func (f *DatasetFeature) DoGetMongoDB(ctx context.Context, cfg *config.Configuration) (store.MongoDB, error) {
	return f.MongoClient, nil
}

func (f *DatasetFeature) DoGetGraphDBOk(ctx context.Context) (store.GraphDB, service.Closer, error) {
	return &storeMock.GraphDBMock{CloseFunc: funcClose}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
}

func (f *DatasetFeature) DoGetKafkaProducerOk(ctx context.Context, cfg *config.Configuration) (kafka.IProducer, error) {
	return &kafkatest.IProducerMock{
		ChannelsFunc: func() *kafka.ProducerChannels {
			return &kafka.ProducerChannels{}
		},
		CloseFunc: funcClose,
	}, nil
}

func (f *DatasetFeature) iHaveTheseDatasets(datasetsJson *godog.DocString) error {

	datasets := []models.Dataset{}
	m := f.MongoClient

	err := json.Unmarshal([]byte(datasetsJson.Content), &datasets)
	if err != nil {
		return err
	}
	s := m.Session.Copy()
	defer s.Close()

	for _, datasetDoc := range datasets {
		if err := f.putDatasetInDatabase(s, datasetDoc); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetFeature) thereAreNoDatasets() error {
	return f.MongoClient.Session.Copy().DB(f.MongoClient.Database).DropDatabase()
}

func (f *DatasetFeature) putDatasetInDatabase(s *mgo.Session, datasetDoc models.Dataset) error {
	datasetID := datasetDoc.ID

	datasetUp := models.DatasetUpdate{
		ID:      datasetID,
		Next:    &datasetDoc,
		Current: &datasetDoc,
	}

	update := bson.M{
		"$set": datasetUp,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}
	_, err := s.DB(f.MongoClient.Database).C("datasets").UpsertId(datasetID, update)
	if err != nil {
		return err
	}
	return nil
}

func (f *DatasetFeature) privateEndpointsAreEnabled() error {
	f.Config.EnablePrivateEndpoints = true
	return nil
}

func (f *DatasetFeature) theDocumentInTheDatabaseForIdShouldBe(documentId string, documentJson *godog.DocString) error {
	s := f.MongoClient.Session.Copy()
	defer s.Close()

	var expectedDataset models.Dataset

	json.Unmarshal([]byte(documentJson.Content), &expectedDataset)

	filterCursor := s.DB(f.MongoClient.Database).C("datasets").FindId(documentId)

	var link models.DatasetUpdate

	if err := filterCursor.One(&link); err != nil {
		return err
	}

	assert.Equal(&f.ErrorFeature, documentId, link.ID)

	document := link.Next

	assert.Equal(&f.ErrorFeature, expectedDataset.Title, document.Title)
	assert.Equal(&f.ErrorFeature, "created", document.State)

	return f.ErrorFeature.StepError()
}
