#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dataset-api
  make test
popd
