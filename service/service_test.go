package service_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/service"
	serviceMock "github.com/ONSdigital/dp-dataset-api/service/mock"
	"github.com/ONSdigital/dp-dataset-api/store"
	storeMock "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/kafkatest"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"
)

var (
	errMongo       = errors.New("MongoDB error")
	errGraph       = errors.New("GraphDB error")
	errKafka       = errors.New("Kafka producer error")
	errServer      = errors.New("HTTP Server error")
	errHealthcheck = errors.New("healthCheck error")
)

var funcDoGetHealthcheckErr = func(*config.Configuration, string, string, string) (service.HealthChecker, error) {
	return nil, errHealthcheck
}

var funcDoGetMongoDBErr = func(context.Context, config.MongoConfig) (store.MongoDB, error) {
	return nil, errMongo
}

var funcDoGetGraphDBErr = func(context.Context) (store.GraphDB, service.Closer, error) {
	return nil, nil, errGraph
}

var funcDoGetKafkaProducerErr = func(context.Context, *config.Configuration, string) (kafka.IProducer, error) {
	return nil, errKafka
}

func TestRun(t *testing.T) {
	Convey("Having a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		cfg.EnablePrivateEndpoints = true
		So(err, ShouldBeNil)

		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(string, healthcheck.Checker) error { return nil },
			StartFunc:    func(context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error {
				serverWg.Done()
				return nil
			},
		}

		failingServerMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error {
				serverWg.Done()
				return errServer
			},
		}

		funcDoGetHealthcheckOk := func(*config.Configuration, string, string, string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		funcDoGetHTTPServer := func(string, http.Handler) service.HTTPServer {
			return serverMock
		}

		funcDoGetFailingHTTPSerer := func(string, http.Handler) service.HTTPServer {
			return failingServerMock
		}

		funcDoGetMongoDBOk := func(context.Context, config.MongoConfig) (store.MongoDB, error) {
			return &storeMock.MongoDBMock{}, nil
		}

		funcDoGetGraphDBOk := func(context.Context) (store.GraphDB, service.Closer, error) {
			var funcClose = func(context.Context) error {
				return nil
			}
			return &storeMock.GraphDBMock{}, &serviceMock.CloserMock{CloseFunc: funcClose}, nil
		}

		funcDoGetKafkaProducerOk := func(context.Context, *config.Configuration, string) (kafka.IProducer, error) {
			return &kafkatest.IProducerMock{
				ChannelsFunc: func() *kafka.ProducerChannels {
					return &kafka.ProducerChannels{}
				},
				LogErrorsFunc: func(context.Context) {
					// Do nothing
				},
			}, nil
		}

		Convey("Given that initialising MongoDB returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc: funcDoGetMongoDBErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errMongo)
				So(svcList.MongoDB, ShouldBeFalse)
				So(svcList.Graph, ShouldBeFalse)
				So(svcList.GenerateDownloadsProducer, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising GraphDB returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc: funcDoGetMongoDBOk,
				DoGetGraphDBFunc: funcDoGetGraphDBErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errGraph)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeFalse)
				So(svcList.GenerateDownloadsProducer, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising Kafka producer returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errKafka)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeTrue)
				So(svcList.GenerateDownloadsProducer, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising Helthcheck returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errHealthcheck)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeTrue)
				So(svcList.GenerateDownloadsProducer, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that Checkers cannot be registered", func() {
			errAddheckFail := errors.New("Error(s) registering checkers for healthcheck")
			hcMockAddFail := &serviceMock.HealthCheckerMock{
				AddCheckFunc: func(string, healthcheck.Checker) error { return errAddheckFail },
				StartFunc:    func(context.Context) {},
			}

			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc: func(*config.Configuration, string, string, string) (service.HealthChecker, error) {
					return hcMockAddFail, nil
				},
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails, but all checks try to register", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("unable to register checkers: %s", errAddheckFail.Error()))
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeTrue)
				So(svcList.GenerateDownloadsProducer, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeTrue)
				So(len(hcMockAddFail.AddCheckCalls()), ShouldEqual, 5)
				So(hcMockAddFail.AddCheckCalls()[0].Name, ShouldResemble, "Zebedee")
				So(hcMockAddFail.AddCheckCalls()[1].Name, ShouldResemble, "Kafka Generate Downloads Producer")
				So(hcMockAddFail.AddCheckCalls()[2].Name, ShouldResemble, "Kafka Generate Cantabular Downloads Producer")
				So(hcMockAddFail.AddCheckCalls()[3].Name, ShouldResemble, "Graph DB")
				So(hcMockAddFail.AddCheckCalls()[4].Name, ShouldResemble, "Mongo DB")
			})
		})

		Convey("Given that all dependencies are successfully initialised", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
				DoGetHTTPServerFunc:    funcDoGetHTTPServer,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			serverWg.Add(1)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run succeeds and all the flags are set", func() {
				So(err, ShouldBeNil)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeTrue)
				So(svcList.GenerateDownloadsProducer, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeTrue)
			})

			Convey("The checkers are registered and the healthcheck and http server started", func() {
				So(len(hcMock.AddCheckCalls()), ShouldEqual, 5)
				So(hcMock.AddCheckCalls()[0].Name, ShouldResemble, "Zebedee")
				So(hcMock.AddCheckCalls()[1].Name, ShouldResemble, "Kafka Generate Downloads Producer")
				So(hcMock.AddCheckCalls()[2].Name, ShouldResemble, "Kafka Generate Cantabular Downloads Producer")
				So(hcMock.AddCheckCalls()[3].Name, ShouldResemble, "Graph DB")
				So(hcMock.AddCheckCalls()[4].Name, ShouldResemble, "Mongo DB")
				So(len(initMock.DoGetHTTPServerCalls()), ShouldEqual, 1)
				So(initMock.DoGetHTTPServerCalls()[0].BindAddr, ShouldEqual, ":22000")
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("Given that all dependencies are successfully initialised, private endpoints are disabled", func() {
			cfg.EnablePrivateEndpoints = false
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
				DoGetHTTPServerFunc:    funcDoGetHTTPServer,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			serverWg.Add(1)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run succeeds and all the flags except Graph are set", func() {
				So(err, ShouldBeNil)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.Graph, ShouldBeFalse)
				So(svcList.GenerateDownloadsProducer, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeTrue)
			})

			Convey("Only the checkers for MongoDB are registered, and the healthcheck and http server started", func() {
				So(len(hcMock.AddCheckCalls()), ShouldEqual, 1)
				So(hcMock.AddCheckCalls()[0].Name, ShouldResemble, "Mongo DB")
				So(len(initMock.DoGetHTTPServerCalls()), ShouldEqual, 1)
				So(initMock.DoGetHTTPServerCalls()[0].BindAddr, ShouldEqual, ":22000")
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("Given that all dependencies are successfully initialised but the http server fails", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
				DoGetHTTPServerFunc:    funcDoGetFailingHTTPSerer,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			serverWg.Add(1)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			Convey("Then the error is returned in the error channel", func() {
				sErr := <-svcErrors
				So(sErr.Error(), ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
				So(len(failingServerMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Having a correctly initialised service", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		hcStopped := false
		serverStopped := false

		// healthcheck Stop does not depend on any other service being closed/stopped
		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(string, healthcheck.Checker) error { return nil },
			StartFunc:    func(context.Context) {},
			StopFunc:     func() { hcStopped = true },
		}

		// server Shutdown will fail if healthcheck is not stopped
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error { return nil },
			ShutdownFunc: func(context.Context) error {
				if !hcStopped {
					return errors.New("Server was stopped before healthcheck")
				}
				serverStopped = true
				return nil
			},
		}

		funcClose := func(context.Context) error {
			if !hcStopped {
				return errors.New("Dependency was closed before healthcheck")
			}
			if !serverStopped {
				return errors.New("Dependency was closed before http server")
			}
			return nil
		}

		// mongoDB will fail if healthcheck or http server are not stopped
		mongoMock := &storeMock.MongoDBMock{
			CloseFunc: funcClose,
		}

		// graphDB will fail if healthcheck or http server are not stopped
		graphMock := &storeMock.GraphDBMock{
			CloseFunc: funcClose,
		}

		graphErrorConsumerMock := &serviceMock.CloserMock{
			CloseFunc: funcClose,
		}

		// Kafka producer will fail if healthcheck or http server are not stopped
		kafkaProducerMock := &kafkatest.IProducerMock{
			ChannelsFunc: func() *kafka.ProducerChannels {
				return &kafka.ProducerChannels{}
			},
			CloseFunc: funcClose,
			LogErrorsFunc: func(context.Context) {
				// Do nothing
			},
		}

		Convey("Closing a service does not close uninitialised dependencies", func() {
			svcList := service.NewServiceList(nil)
			svcList.HealthCheck = true
			svc := service.New(cfg, svcList)
			svc.SetServer(serverMock)
			svc.SetHealthCheck(hcMock)
			err = svc.Close(context.Background())
			So(err, ShouldBeNil)
			So(len(hcMock.StopCalls()), ShouldEqual, 1)
			So(len(serverMock.ShutdownCalls()), ShouldEqual, 1)
		})

		fullSvcList := &service.ExternalServiceList{
			GenerateDownloadsProducer: true,
			Graph:                     true,
			HealthCheck:               true,
			MongoDB:                   true,
			Init:                      nil,
		}

		Convey("Closing the service results in all the initialised dependencies being closed in the expected order", func() {
			svc := service.New(cfg, fullSvcList)
			svc.SetServer(serverMock)
			svc.SetHealthCheck(hcMock)
			svc.SetDownloadsProducer(kafkaProducerMock)
			svc.SetMongoDB(mongoMock)
			svc.SetGraphDB(graphMock)
			svc.SetGraphDBErrorConsumer(graphErrorConsumerMock)
			err = svc.Close(context.Background())
			So(err, ShouldBeNil)
			So(len(hcMock.StopCalls()), ShouldEqual, 1)
			So(len(serverMock.ShutdownCalls()), ShouldEqual, 1)
			So(len(mongoMock.CloseCalls()), ShouldEqual, 1)
			So(len(graphMock.CloseCalls()), ShouldEqual, 1)
			So(len(graphErrorConsumerMock.CloseCalls()), ShouldEqual, 1)
			So(len(kafkaProducerMock.CloseCalls()), ShouldEqual, 1)
		})

		Convey("If services fail to stop, the Close operation tries to close all dependencies and returns an error", func() {
			failingserverMock := &serviceMock.HTTPServerMock{
				ListenAndServeFunc: func() error { return nil },
				ShutdownFunc: func(context.Context) error {
					return errors.New("Failed to stop http server")
				},
			}

			svc := service.New(cfg, fullSvcList)
			svc.SetServer(failingserverMock)
			svc.SetHealthCheck(hcMock)
			svc.SetDownloadsProducer(kafkaProducerMock)
			svc.SetMongoDB(mongoMock)
			svc.SetGraphDB(graphMock)
			svc.SetGraphDBErrorConsumer(graphErrorConsumerMock)
			err = svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldResemble, "failed to shutdown gracefully")
			So(len(hcMock.StopCalls()), ShouldEqual, 1)
			So(len(failingserverMock.ShutdownCalls()), ShouldEqual, 1)
			So(len(mongoMock.CloseCalls()), ShouldEqual, 1)
			So(len(graphMock.CloseCalls()), ShouldEqual, 1)
			So(len(graphErrorConsumerMock.CloseCalls()), ShouldEqual, 1)
			So(len(kafkaProducerMock.CloseCalls()), ShouldEqual, 1)
		})
	})
}
