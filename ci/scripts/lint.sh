#!/bin/bash -eux

pushd dp-dataset-api
  make lint
  ruff check ./sdk/python
popd
