#!/usr/bin/env bash

mongoimport --host 127.0.0.1:27017 --db datasets --collection datasets --file datasets.json -vvvvvv
mongoimport --host 127.0.0.1:27017 --db datasets --collection editions --file editions.json
mongoimport --host 127.0.0.1:27017 --db datasets --collection instances --file instances.json
mongoimport --host 127.0.0.1:27017 --db datasets --collection dimension.options --file dimension.options.json
