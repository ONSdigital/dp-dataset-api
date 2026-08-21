#!/bin/bash -eux

pushd dp-dataset-api
  make test-go
  pip install poetry
  poetry -C sdk/python install
  make -C sdk/python test
popd
