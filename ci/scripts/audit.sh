#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-dataset-api
  make audit-go
  pip-audit ./sdk/python
popd