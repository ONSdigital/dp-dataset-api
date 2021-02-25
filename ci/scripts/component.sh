#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-dataset-api
  export MEMONGO_DOWNLOAD_URL=https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1804-4.0.23.tgz
  make test-component
popd
