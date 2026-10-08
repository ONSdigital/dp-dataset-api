#!/bin/bash -eux

pushd dp-dataset-api
  pip install poetry
  make -C sdk/python install-dev
  make test
popd
