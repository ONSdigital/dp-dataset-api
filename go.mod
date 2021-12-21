module github.com/ONSdigital/dp-dataset-api

go 1.17

replace github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.2.0
	github.com/ONSdigital/dp-assistdog v0.0.1
	github.com/ONSdigital/dp-authorisation v0.2.0
	github.com/ONSdigital/dp-component-test v0.6.3
	github.com/ONSdigital/dp-graph/v2 v2.7.1
	github.com/ONSdigital/dp-healthcheck v1.2.1
	github.com/ONSdigital/dp-kafka/v2 v2.4.3
	github.com/ONSdigital/dp-mongodb/v3 v3.0.0-beta.5
	github.com/ONSdigital/dp-net/v2 v2.2.0-beta
	github.com/ONSdigital/log.go/v2 v2.0.9
	github.com/cucumber/godog v0.11.0
	github.com/google/go-cmp v0.5.5
	github.com/gorilla/mux v1.8.0
	github.com/justinas/alice v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/satori/go.uuid v1.2.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/stretchr/testify v1.7.0
	go.mongodb.org/mongo-driver v1.8.0
)
