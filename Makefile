SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=.

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

export ENABLE_PRIVATE_ENDPOINTS?=true

build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build -o $(BUILD_ARCH)/$(BIN_DIR)/dp-dataset-api main.go
debug:
	GRAPH_DRIVER_TYPE="neo4j" GRAPH_ADDR="bolt://localhost:7687" HUMAN_LOG=1 go run main.go
acceptance-publishing: build
	ENABLE_PRIVATE_ENDPOINTS=true MONGODB_DATABASE=test HUMAN_LOG=1 go run main.go
acceptance-web: build
	ENABLE_PRIVATE_ENDPOINTS=false MONGODB_DATABASE=test HUMAN_LOG=1 go run main.go
test:
	go test -cover $(shell go list ./... | grep -v /vendor/)

.PHONEY: test build debug
