dp-dataset-api
==================
A ONS API used to navigate datasets which are published.

#### Database
* Run ```brew install mongo```
* Run ```brew services start mongodb```
* Run ```./scripts/InitDatabase.sh```

### Healthcheck

The endpoint `/healthcheck` checks the connection to the database and returns
one of:

- success (200, JSON "status": "OK")
- failure (500, JSON "status": "error").

### Configuration

| Environment variable       | Default                              | Description
| -------------------------- | -------------------------------------| -----------
| BIND_ADDR                  | :22000                               | The host and port to bind to
| MONGODB_BIND_ADDR          | localhost:27017                      | The MongoDB bind address
| MONGODB_DATABASE           | datasets                             | The MongoDB dataset database
| MONGODB_COLLECTION         | datasets                             | MongoDB collection
| SECRET_KEY                 | FD0108EA-825D-411C-9B1D-41EF7727F465 | A secret key used authentication
| CODE_LIST_API_URL          | http://localhost:22400               | The host name for the Dataset API
| DATASET_API_URL            | http://localhost:22000               | The host name for the CodeList API
| WEBSITE_URL                | http://localhost:20000               | The host name for the website
| GRACEFUL_SHUTDOWN_TIMEOUT  | 5s                                   | The graceful shutdown timeout
| HEALTHCHECK_TIMEOUT        | 2s                                   | The timeout that the healthcheck allows for checked subsystems

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details
