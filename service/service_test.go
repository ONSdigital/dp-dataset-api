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
	"github.com/smartystreets/goconvey/convey"
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
	convey.Convey("Having a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		cfg.EnablePrivateEndpoints = true
		convey.So(err, convey.ShouldBeNil)

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

		convey.Convey("Given that initialising MongoDB returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc: funcDoGetMongoDBErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			convey.Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				convey.So(err, convey.ShouldResemble, errMongo)
				convey.So(svcList.MongoDB, convey.ShouldBeFalse)
				convey.So(svcList.Graph, convey.ShouldBeFalse)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeFalse)
				convey.So(svcList.HealthCheck, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Given that initialising GraphDB returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc: funcDoGetMongoDBOk,
				DoGetGraphDBFunc: funcDoGetGraphDBErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			convey.Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				convey.So(err, convey.ShouldResemble, errGraph)
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeFalse)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeFalse)
				convey.So(svcList.HealthCheck, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Given that initialising Kafka producer returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetGraphDBFunc:       funcDoGetGraphDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc := service.New(cfg, svcList)
			err := svc.Run(ctx, testBuildTime, testGitCommit, testVersion, svcErrors)

			convey.Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				convey.So(err, convey.ShouldResemble, errKafka)
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeTrue)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeFalse)
				convey.So(svcList.HealthCheck, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Given that initialising Helthcheck returns an error", func() {
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

			convey.Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				convey.So(err, convey.ShouldResemble, errHealthcheck)
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeTrue)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeTrue)
				convey.So(svcList.HealthCheck, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Given that Checkers cannot be registered", func() {
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

			convey.Convey("Then service Run fails, but all checks try to register", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldResemble, fmt.Sprintf("unable to register checkers: %s", errAddheckFail.Error()))
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeTrue)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeTrue)
				convey.So(svcList.HealthCheck, convey.ShouldBeTrue)
				convey.So(len(hcMockAddFail.AddCheckCalls()), convey.ShouldEqual, 5)
				convey.So(hcMockAddFail.AddCheckCalls()[0].Name, convey.ShouldResemble, "Zebedee")
				convey.So(hcMockAddFail.AddCheckCalls()[1].Name, convey.ShouldResemble, "Kafka Generate Downloads Producer")
				convey.So(hcMockAddFail.AddCheckCalls()[2].Name, convey.ShouldResemble, "Kafka Generate Cantabular Downloads Producer")
				convey.So(hcMockAddFail.AddCheckCalls()[3].Name, convey.ShouldResemble, "Graph DB")
				convey.So(hcMockAddFail.AddCheckCalls()[4].Name, convey.ShouldResemble, "Mongo DB")
			})
		})

		convey.Convey("Given that all dependencies are successfully initialised", func() {
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

			convey.Convey("Then service Run succeeds and all the flags are set", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeTrue)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeTrue)
				convey.So(svcList.HealthCheck, convey.ShouldBeTrue)
			})

			convey.Convey("The checkers are registered and the healthcheck and http server started", func() {
				convey.So(len(hcMock.AddCheckCalls()), convey.ShouldEqual, 5)
				convey.So(hcMock.AddCheckCalls()[0].Name, convey.ShouldResemble, "Zebedee")
				convey.So(hcMock.AddCheckCalls()[1].Name, convey.ShouldResemble, "Kafka Generate Downloads Producer")
				convey.So(hcMock.AddCheckCalls()[2].Name, convey.ShouldResemble, "Kafka Generate Cantabular Downloads Producer")
				convey.So(hcMock.AddCheckCalls()[3].Name, convey.ShouldResemble, "Graph DB")
				convey.So(hcMock.AddCheckCalls()[4].Name, convey.ShouldResemble, "Mongo DB")
				convey.So(len(initMock.DoGetHTTPServerCalls()), convey.ShouldEqual, 1)
				convey.So(initMock.DoGetHTTPServerCalls()[0].BindAddr, convey.ShouldEqual, ":22000")
				convey.So(len(hcMock.StartCalls()), convey.ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				convey.So(len(serverMock.ListenAndServeCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("Given that all dependencies are successfully initialised, private endpoints are disabled", func() {
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

			convey.Convey("Then service Run succeeds and all the flags except Graph are set", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(svcList.MongoDB, convey.ShouldBeTrue)
				convey.So(svcList.Graph, convey.ShouldBeFalse)
				convey.So(svcList.GenerateDownloadsProducer, convey.ShouldBeFalse)
				convey.So(svcList.HealthCheck, convey.ShouldBeTrue)
			})

			convey.Convey("Only the checkers for MongoDB are registered, and the healthcheck and http server started", func() {
				convey.So(len(hcMock.AddCheckCalls()), convey.ShouldEqual, 1)
				convey.So(hcMock.AddCheckCalls()[0].Name, convey.ShouldResemble, "Mongo DB")
				convey.So(len(initMock.DoGetHTTPServerCalls()), convey.ShouldEqual, 1)
				convey.So(initMock.DoGetHTTPServerCalls()[0].BindAddr, convey.ShouldEqual, ":22000")
				convey.So(len(hcMock.StartCalls()), convey.ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				convey.So(len(serverMock.ListenAndServeCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("Given that all dependencies are successfully initialised but the http server fails", func() {
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
			convey.So(err, convey.ShouldBeNil)

			convey.Convey("Then the error is returned in the error channel", func() {
				sErr := <-svcErrors
				convey.So(sErr.Error(), convey.ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
				convey.So(len(failingServerMock.ListenAndServeCalls()), convey.ShouldEqual, 1)
			})
		})
	})
}

func TestClose(t *testing.T) {
	convey.Convey("Having a correctly initialised service", t, func() {
		cfg, err := config.Get()
		convey.So(err, convey.ShouldBeNil)

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

		convey.Convey("Closing a service does not close uninitialised dependencies", func() {
			svcList := service.NewServiceList(nil)
			svcList.HealthCheck = true
			svc := service.New(cfg, svcList)
			svc.SetServer(serverMock)
			svc.SetHealthCheck(hcMock)
			err = svc.Close(context.Background())
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(hcMock.StopCalls()), convey.ShouldEqual, 1)
			convey.So(len(serverMock.ShutdownCalls()), convey.ShouldEqual, 1)
		})

		fullSvcList := &service.ExternalServiceList{
			GenerateDownloadsProducer: true,
			Graph:                     true,
			HealthCheck:               true,
			MongoDB:                   true,
			Init:                      nil,
		}

		convey.Convey("Closing the service results in all the initialised dependencies being closed in the expected order", func() {
			svc := service.New(cfg, fullSvcList)
			svc.SetServer(serverMock)
			svc.SetHealthCheck(hcMock)
			svc.SetDownloadsProducer(kafkaProducerMock)
			svc.SetMongoDB(mongoMock)
			svc.SetGraphDB(graphMock)
			svc.SetGraphDBErrorConsumer(graphErrorConsumerMock)
			err = svc.Close(context.Background())
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(hcMock.StopCalls()), convey.ShouldEqual, 1)
			convey.So(len(serverMock.ShutdownCalls()), convey.ShouldEqual, 1)
			convey.So(len(mongoMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(graphMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(graphErrorConsumerMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(kafkaProducerMock.CloseCalls()), convey.ShouldEqual, 1)
		})

		convey.Convey("If services fail to stop, the Close operation tries to close all dependencies and returns an error", func() {
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
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldResemble, "failed to shutdown gracefully")
			convey.So(len(hcMock.StopCalls()), convey.ShouldEqual, 1)
			convey.So(len(failingserverMock.ShutdownCalls()), convey.ShouldEqual, 1)
			convey.So(len(mongoMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(graphMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(graphErrorConsumerMock.CloseCalls()), convey.ShouldEqual, 1)
			convey.So(len(kafkaProducerMock.CloseCalls()), convey.ShouldEqual, 1)
		})
	})
}
