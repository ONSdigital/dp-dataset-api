#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-dataset-api
  pip install poetry
  make -C sdk/python install-dev
  make audit
popd