module github.com/ONSdigital/dp-dataset-api

go 1.15

replace github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible

require (
	github.com/ONSdigital/dp-api-clients-go v1.43.0 // indirect
	github.com/ONSdigital/dp-api-clients-go/v2 v2.2.0
	github.com/ONSdigital/dp-authorisation v0.2.0
	github.com/ONSdigital/dp-component-test v0.6.0
	github.com/ONSdigital/dp-graph/v2 v2.7.1
	github.com/ONSdigital/dp-healthcheck v1.1.2
	github.com/ONSdigital/dp-kafka/v2 v2.4.1
	github.com/ONSdigital/dp-mongodb/v3 v3.0.0-beta.1
	github.com/ONSdigital/dp-net/v2 v2.2.0-beta
	github.com/ONSdigital/go-ns v0.0.0-20200902154605-290c8b5ba5eb // indirect
	github.com/ONSdigital/log.go/v2 v2.0.9
	github.com/cucumber/godog v0.11.0
	github.com/go-test/deep v1.0.7 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-memdb v1.3.1 // indirect
	github.com/justinas/alice v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.13.4 // indirect
	github.com/pkg/errors v0.9.1
	github.com/satori/go.uuid v1.2.0
	github.com/smartystreets/goconvey v1.6.4
	github.com/stretchr/testify v1.7.0
	go.mongodb.org/mongo-driver v1.7.1
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
)
