#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dataset-api
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0
  make lint
popd
