#!/bin/bash -eux

cwd=$(pwd)

export GOPATH=$cwd/go

pushd $GOPATH/src/github.com/ONSdigital/dp-dataset-api
  make build && mv build/$(go env GOOS)-$(go env GOARCH)/* $cwd/build
popd
