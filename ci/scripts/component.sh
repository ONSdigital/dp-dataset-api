#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dataset-api
  make test-component
popd