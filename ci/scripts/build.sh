#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dataset-api
  make build && mv build/dp-dataset-api $cwd/build
  cp Dockerfile.concourse $cwd/build
popd
