#!/bin/bash -eux

pushd dp-dataset-api
  make lint-go
  ruff check ./sdk/python
popd
