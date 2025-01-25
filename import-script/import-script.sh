#!/usr/bin/env bash

mongoimport --db datasets --collection datasets --file datasets.json
mongoimport --db datasets --collection editions --file editions.json
mongoimport --db datasets --collection instances --file instances.json
mongoimport --db datasets --collection dimension.options --file dimension.options.json
