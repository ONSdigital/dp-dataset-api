// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/service"
	"github.com/ONSdigital/dp-dataset-api/store"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"net/http"
	"sync"
)

// Ensure, that InitialiserMock does implement service.Initialiser.
// If this is not the case, regenerate this file with moq.
var _ service.Initialiser = &InitialiserMock{}

// InitialiserMock is a mock implementation of service.Initialiser.
//
// 	func TestSomethingThatUsesInitialiser(t *testing.T) {
//
// 		// make and configure a mocked service.Initialiser
// 		mockedInitialiser := &InitialiserMock{
// 			DoGetGraphDBFunc: func(ctx context.Context) (store.GraphDB, service.Closer, error) {
// 				panic("mock out the DoGetGraphDB method")
// 			},
// 			DoGetHTTPServerFunc: func(bindAddr string, router http.Handler) service.HTTPServer {
// 				panic("mock out the DoGetHTTPServer method")
// 			},
// 			DoGetHealthCheckFunc: func(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
// 				panic("mock out the DoGetHealthCheck method")
// 			},
// 			DoGetKafkaProducerFunc: func(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error) {
// 				panic("mock out the DoGetKafkaProducer method")
// 			},
// 			DoGetMongoDBFunc: func(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error) {
// 				panic("mock out the DoGetMongoDB method")
// 			},
// 		}
//
// 		// use mockedInitialiser in code that requires service.Initialiser
// 		// and then make assertions.
//
// 	}
type InitialiserMock struct {
	// DoGetGraphDBFunc mocks the DoGetGraphDB method.
	DoGetGraphDBFunc func(ctx context.Context) (store.GraphDB, service.Closer, error)

	// DoGetHTTPServerFunc mocks the DoGetHTTPServer method.
	DoGetHTTPServerFunc func(bindAddr string, router http.Handler) service.HTTPServer

	// DoGetHealthCheckFunc mocks the DoGetHealthCheck method.
	DoGetHealthCheckFunc func(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error)

	// DoGetKafkaProducerFunc mocks the DoGetKafkaProducer method.
	DoGetKafkaProducerFunc func(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error)

	// DoGetMongoDBFunc mocks the DoGetMongoDB method.
	DoGetMongoDBFunc func(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error)

	// calls tracks calls to the methods.
	calls struct {
		// DoGetGraphDB holds details about calls to the DoGetGraphDB method.
		DoGetGraphDB []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// DoGetHTTPServer holds details about calls to the DoGetHTTPServer method.
		DoGetHTTPServer []struct {
			// BindAddr is the bindAddr argument value.
			BindAddr string
			// Router is the router argument value.
			Router http.Handler
		}
		// DoGetHealthCheck holds details about calls to the DoGetHealthCheck method.
		DoGetHealthCheck []struct {
			// Cfg is the cfg argument value.
			Cfg *config.Configuration
			// BuildTime is the buildTime argument value.
			BuildTime string
			// GitCommit is the gitCommit argument value.
			GitCommit string
			// Version is the version argument value.
			Version string
		}
		// DoGetKafkaProducer holds details about calls to the DoGetKafkaProducer method.
		DoGetKafkaProducer []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Cfg is the cfg argument value.
			Cfg *config.Configuration
			// Topic is the topic argument value.
			Topic string
		}
		// DoGetMongoDB holds details about calls to the DoGetMongoDB method.
		DoGetMongoDB []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Cfg is the cfg argument value.
			Cfg config.MongoConfig
		}
	}
	lockDoGetGraphDB       sync.RWMutex
	lockDoGetHTTPServer    sync.RWMutex
	lockDoGetHealthCheck   sync.RWMutex
	lockDoGetKafkaProducer sync.RWMutex
	lockDoGetMongoDB       sync.RWMutex
}

// DoGetGraphDB calls DoGetGraphDBFunc.
func (mock *InitialiserMock) DoGetGraphDB(ctx context.Context) (store.GraphDB, service.Closer, error) {
	if mock.DoGetGraphDBFunc == nil {
		panic("InitialiserMock.DoGetGraphDBFunc: method is nil but Initialiser.DoGetGraphDB was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	mock.lockDoGetGraphDB.Lock()
	mock.calls.DoGetGraphDB = append(mock.calls.DoGetGraphDB, callInfo)
	mock.lockDoGetGraphDB.Unlock()
	return mock.DoGetGraphDBFunc(ctx)
}

// DoGetGraphDBCalls gets all the calls that were made to DoGetGraphDB.
// Check the length with:
//     len(mockedInitialiser.DoGetGraphDBCalls())
func (mock *InitialiserMock) DoGetGraphDBCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	mock.lockDoGetGraphDB.RLock()
	calls = mock.calls.DoGetGraphDB
	mock.lockDoGetGraphDB.RUnlock()
	return calls
}

// DoGetHTTPServer calls DoGetHTTPServerFunc.
func (mock *InitialiserMock) DoGetHTTPServer(bindAddr string, router http.Handler) service.HTTPServer {
	if mock.DoGetHTTPServerFunc == nil {
		panic("InitialiserMock.DoGetHTTPServerFunc: method is nil but Initialiser.DoGetHTTPServer was just called")
	}
	callInfo := struct {
		BindAddr string
		Router   http.Handler
	}{
		BindAddr: bindAddr,
		Router:   router,
	}
	mock.lockDoGetHTTPServer.Lock()
	mock.calls.DoGetHTTPServer = append(mock.calls.DoGetHTTPServer, callInfo)
	mock.lockDoGetHTTPServer.Unlock()
	return mock.DoGetHTTPServerFunc(bindAddr, router)
}

// DoGetHTTPServerCalls gets all the calls that were made to DoGetHTTPServer.
// Check the length with:
//     len(mockedInitialiser.DoGetHTTPServerCalls())
func (mock *InitialiserMock) DoGetHTTPServerCalls() []struct {
	BindAddr string
	Router   http.Handler
} {
	var calls []struct {
		BindAddr string
		Router   http.Handler
	}
	mock.lockDoGetHTTPServer.RLock()
	calls = mock.calls.DoGetHTTPServer
	mock.lockDoGetHTTPServer.RUnlock()
	return calls
}

// DoGetHealthCheck calls DoGetHealthCheckFunc.
func (mock *InitialiserMock) DoGetHealthCheck(cfg *config.Configuration, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
	if mock.DoGetHealthCheckFunc == nil {
		panic("InitialiserMock.DoGetHealthCheckFunc: method is nil but Initialiser.DoGetHealthCheck was just called")
	}
	callInfo := struct {
		Cfg       *config.Configuration
		BuildTime string
		GitCommit string
		Version   string
	}{
		Cfg:       cfg,
		BuildTime: buildTime,
		GitCommit: gitCommit,
		Version:   version,
	}
	mock.lockDoGetHealthCheck.Lock()
	mock.calls.DoGetHealthCheck = append(mock.calls.DoGetHealthCheck, callInfo)
	mock.lockDoGetHealthCheck.Unlock()
	return mock.DoGetHealthCheckFunc(cfg, buildTime, gitCommit, version)
}

// DoGetHealthCheckCalls gets all the calls that were made to DoGetHealthCheck.
// Check the length with:
//     len(mockedInitialiser.DoGetHealthCheckCalls())
func (mock *InitialiserMock) DoGetHealthCheckCalls() []struct {
	Cfg       *config.Configuration
	BuildTime string
	GitCommit string
	Version   string
} {
	var calls []struct {
		Cfg       *config.Configuration
		BuildTime string
		GitCommit string
		Version   string
	}
	mock.lockDoGetHealthCheck.RLock()
	calls = mock.calls.DoGetHealthCheck
	mock.lockDoGetHealthCheck.RUnlock()
	return calls
}

// DoGetKafkaProducer calls DoGetKafkaProducerFunc.
func (mock *InitialiserMock) DoGetKafkaProducer(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error) {
	if mock.DoGetKafkaProducerFunc == nil {
		panic("InitialiserMock.DoGetKafkaProducerFunc: method is nil but Initialiser.DoGetKafkaProducer was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		Cfg   *config.Configuration
		Topic string
	}{
		Ctx:   ctx,
		Cfg:   cfg,
		Topic: topic,
	}
	mock.lockDoGetKafkaProducer.Lock()
	mock.calls.DoGetKafkaProducer = append(mock.calls.DoGetKafkaProducer, callInfo)
	mock.lockDoGetKafkaProducer.Unlock()
	return mock.DoGetKafkaProducerFunc(ctx, cfg, topic)
}

// DoGetKafkaProducerCalls gets all the calls that were made to DoGetKafkaProducer.
// Check the length with:
//     len(mockedInitialiser.DoGetKafkaProducerCalls())
func (mock *InitialiserMock) DoGetKafkaProducerCalls() []struct {
	Ctx   context.Context
	Cfg   *config.Configuration
	Topic string
} {
	var calls []struct {
		Ctx   context.Context
		Cfg   *config.Configuration
		Topic string
	}
	mock.lockDoGetKafkaProducer.RLock()
	calls = mock.calls.DoGetKafkaProducer
	mock.lockDoGetKafkaProducer.RUnlock()
	return calls
}

// DoGetMongoDB calls DoGetMongoDBFunc.
func (mock *InitialiserMock) DoGetMongoDB(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error) {
	if mock.DoGetMongoDBFunc == nil {
		panic("InitialiserMock.DoGetMongoDBFunc: method is nil but Initialiser.DoGetMongoDB was just called")
	}
	callInfo := struct {
		Ctx context.Context
		Cfg config.MongoConfig
	}{
		Ctx: ctx,
		Cfg: cfg,
	}
	mock.lockDoGetMongoDB.Lock()
	mock.calls.DoGetMongoDB = append(mock.calls.DoGetMongoDB, callInfo)
	mock.lockDoGetMongoDB.Unlock()
	return mock.DoGetMongoDBFunc(ctx, cfg)
}

// DoGetMongoDBCalls gets all the calls that were made to DoGetMongoDB.
// Check the length with:
//     len(mockedInitialiser.DoGetMongoDBCalls())
func (mock *InitialiserMock) DoGetMongoDBCalls() []struct {
	Ctx context.Context
	Cfg config.MongoConfig
} {
	var calls []struct {
		Ctx context.Context
		Cfg config.MongoConfig
	}
	mock.lockDoGetMongoDB.RLock()
	calls = mock.calls.DoGetMongoDB
	mock.lockDoGetMongoDB.RUnlock()
	return calls
}
