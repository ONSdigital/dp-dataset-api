package service

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
)

//go:generate moq -out mock/initialiser.go -pkg mock . Initialiser
//go:generate moq -out mock/server.go -pkg mock . HTTPServer
//go:generate moq -out mock/healthcheck.go -pkg mock . HealthChecker
//go:generate moq -out mock/closer.go -pkg mock . Closer
//go:generate moq -out mock/cantabularclient.go -pkg mock . CantabularClient

// Initialiser defines the methods to initialise external services
type Initialiser interface {
	DoGetHTTPServer(bindAddr string, router http.Handler) HTTPServer
	DoGetHealthCheck(cfg *config.Configuration, buildTime, gitCommit, version string) (HealthChecker, error)
	DoGetKafkaProducer(ctx context.Context, cfg *config.Configuration, topic string) (kafka.IProducer, error)
	DoGetGraphDB(ctx context.Context) (store.GraphDB, Closer, error)
	DoGetMongoDB(ctx context.Context, cfg config.MongoConfig) (store.MongoDB, error)
	DoGetCantabularClient(ctx context.Context, cfg config.CantabularConfig) CantabularClient
}

// HTTPServer defines the required methods from the HTTP server
type HTTPServer interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
}

// HealthChecker defines the required methods from Healthcheck
type HealthChecker interface {
	Handler(w http.ResponseWriter, req *http.Request)
	Start(ctx context.Context)
	Stop()
	AddCheck(name string, checker healthcheck.Checker) (err error)
}

// Closer defines the required methods for a closable resource
type Closer interface {
	Close(ctx context.Context) error
}

type CantabularClient interface {
	PopulationTypes(ctx context.Context) []store.CantabularBlob
	//StaticDatasetQueryStreamCSV(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume cantabular.Consumer) (rowCount int32, err error)
	//Checker(context.Context, *healthcheck.CheckState) error
	//CheckerAPIExt(ctx context.Context, state *healthcheck.CheckState) error
}
