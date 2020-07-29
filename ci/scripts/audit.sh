#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-dataset-api
  make audit
popd