SHELL=bash

BUILD=build
BIN_DIR?=.

BUILD_TIME=$(shell date +%s)
GIT_COMMIT=$(shell git rev-parse HEAD)
VERSION ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)
LDFLAGS=-ldflags "-X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT) -X main.Version=$(VERSION)"

export GRAPH_DRIVER_TYPE?=neo4j
export GRAPH_ADDR?=bolt://localhost:7687
export ENABLE_PRIVATE_ENDPOINTS?=true

.PHONY: all
all: audit test build

.PHONY: audit
audit:
	go list -m all | nancy sleuth

.PHONY: build
build:
	@mkdir -p $(BUILD)/$(BIN_DIR)
	go build $(LDFLAGS) -o $(BUILD)/$(BIN_DIR)/dp-dataset-api main.go

.PHONY: debug
debug:
	HUMAN_LOG=1 go run -race $(LDFLAGS) main.go

.PHONY: acceptance-publishing
acceptance-publishing: build
	ENABLE_PRIVATE_ENDPOINTS=true MONGODB_DATABASE=test HUMAN_LOG=1 go run -race $(LDFLAGS) main.go

.PHONY: acceptance-web
acceptance-web: build
	ENABLE_PRIVATE_ENDPOINTS=false MONGODB_DATABASE=test HUMAN_LOG=1 go run -race $(LDFLAGS) main.go

.PHONY: lint
lint:
	exit

.PHONY: test
test:
	go test -race -cover ./...


.PHONY: test-component
test-component:
	ENABLE_PRIVATE_ENDPOINTS=true cd features/compose; docker-compose up --abort-on-container-exit

.PHONY: nomis
nomis:
	go run NOMIS/nomis.go -mongo-url=localhost:27017

.PHONY: test build debug


