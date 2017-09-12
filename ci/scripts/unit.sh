#!/bin/bash -eux

export GOPATH=$(pwd)/go

pushd $GOPATH/src/github.com/ONSdigital/dp-dataset-api
  make test
popd
