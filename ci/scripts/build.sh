#!/bin/bash -eux

pushd dp-dataset-api
  make build
  cp build/dp-dataset-api Dockerfile.concourse ../build
popd
