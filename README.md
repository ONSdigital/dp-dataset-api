# dp-dataset-api

An ONS API used to navigate datasets, editions and versions - which are published.

## Installation

### Database

* Run `brew install mongo`
* Run `brew services start mongodb`

* Run `brew install neo4j`
* Configure neo4j, edit `/usr/local/Cellar/neo4j/3.2.0/libexec/conf/neo4j.conf`
* Set `dbms.security.auth_enabled=false`
* Run `brew services restart neo4j`

### Getting started

* Run api auth stub, [see documentation](https://github.com/ONSdigital/dp-auth-api-stub)
* Run `make debug`

### State changes

Normal sequential order of states:

1. `created` (only on *instance*)
2. `submitted` (only on *instance*)
3. `completed` (only on *instance*)
4. `edition-confirmed` (only on *instance* - this will create an *edition* and *version*, in other words the *instance*
   will now be accessible by `version` endpoints). Also the dataset `next` sub-document will also get updated here and
   so will the *edition*
   (authorised users will see a different latest *version* link versus unauthorised users)
5. `associated` (only on *version*) - dataset `next` sub-document will be updated again and so will the *edition*
6. `published` (only on *version*) - both *edition* and *dataset* are updated - must not be changed

There is the possibility to **rollback** from `associated`  to `edition-confirmed`
where a PST user has attached the *version* to the wrong collection and so not only does the `collection_id` need to be
updated with the new one (or removed altogether)
but the state will need to revert back to `edition-confirmed`.

Lastly, **skipping a state**: it is possibly to jump from `edition-confirmed` to `published`
as long as all the mandatory fields are there. There also might be a scenario whereby the state can change
from `created` to `completed`, missing out the step to `submitted`
due to race conditions, this is not expected to happen, the path to get to `completed` is longer than the `submitted`
one.

### Healthcheck

The endpoint `/health` checks the connection to the database and returns one of:

* success (200, JSON "status": "OK")
* failure (500, JSON "status": "error").

The `/health` endpoint replaces `/healthcheck`, which now returns a `404 Not Found` response.

### Kafka scripts

Scripts for updating and debugging Kafka can be found [here](https://github.com/ONSdigital/dp-data-tools)(dp-data-tools)

### Configuration

| Environment variable               | Default                                                                                                                                                                                                   | Description                                                                                          |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| BIND_ADDR                          | `:22000`                                                                                                                                                                                                  | The host and port to bind to                                                                         |
| MONGODB_BIND_ADDR                  | localhost:27017                                                                                                                                                                                           | The MongoDB bind address                                                                             |
| MONGODB_USERNAME                   |                                                                                                                                                                                                           | The MongoDB Username                                                                                 |
| MONGODB_PASSWORD                   |                                                                                                                                                                                                           | The MongoDB Password                                                                                 |
| MONGODB_DATABASE                   | datasets                                                                                                                                                                                                  | The MongoDB database                                                                                 |
| MONGODB_COLLECTIONS                | DatasetsCollection:datasets, ContactsCollection:contacts, EditionsCollection:editions, InstanceCollection:instances, DimensionOptionsCollection:dimension.options, InstanceLockCollection:instances_locks | The MongoDB collections                                                                              |
| MONGODB_REPLICA_SET                |                                                                                                                                                                                                           | The name of the MongoDB replica set                                                                  |
| MONGODB_ENABLE_READ_CONCERN        | false                                                                                                                                                                                                     | Switch to use (or not) majority read concern                                                         |
| MONGODB_ENABLE_WRITE_CONCERN       | true                                                                                                                                                                                                      | Switch to use (or not) majority write concern                                                        |
| MONGODB_CONNECT_TIMEOUT            | 5s                                                                                                                                                                                                        | The timeout when connecting to MongoDB (`time.Duration` format)                                      |
| MONGODB_QUERY_TIMEOUT              | 15s                                                                                                                                                                                                       | The timeout for querying MongoDB (`time.Duration` format)                                            |
| MONGODB_IS_SSL                     | false                                                                                                                                                                                                     | Switch to use (or not) TLS when connecting to mongodb                                                |
| SECRET_KEY                         | `FD0108EA-825D-411C-9B1D-41EF7727F465`                                                                                                                                                                    | A secret key used authentication                                                                     |
| CODE_LIST_API_URL                  | `http://localhost:22400`                                                                                                                                                                                  | The host name for the CodeList API                                                                   |
| DATASET_API_URL                    | `http://localhost:22000`                                                                                                                                                                                  | The host name for the Dataset API                                                                    |
| GRACEFUL_SHUTDOWN_TIMEOUT          | `5s`                                                                                                                                                                                                      | The graceful shutdown timeout in seconds                                                             |
| WEBSITE_URL                        | `http://localhost:20000`                                                                                                                                                                                  | The host name for the website                                                                        |
| KAFKA_ADDR                         | `localhost:9092`                                                                                                                                                                                          | The address of (TLS-ready) Kafka brokers (comma-separated values)                                    |
| KAFKA_CONSUMER_MIN_BROKERS_HEALTHY | 2                                                                                                                                                                                                         | The minimum number of consumer brokers needed                                                        |
| KAFKA_PRODUCER_MIN_BROKERS_HEALTHY | 2                                                                                                                                                                                                         | The minimum number of producer brokers needed                                                        |
| KAFKA_VERSION                      | `1.0.2`                                                                                                                                                                                                   | The version of (TLS-ready) Kafka                                                                     |
| KAFKA_SEC_PROTO                    | *unset*                 (only `TLS`)                                                                                                                                                                      | if set to `TLS`, kafka connections will use TLS                                                      |
| KAFKA_SEC_CLIENT_KEY               | *unset*                                                                                                                                                                                                   | PEM [2] for the client key (optional, used for client auth) [1]                                      |
| KAFKA_SEC_CLIENT_CERT              | *unset*                                                                                                                                                                                                   | PEM [2] for the client certificate (optional, used for client auth) [1]                              |
| KAFKA_SEC_CA_CERTS                 | *unset*                                                                                                                                                                                                   | PEM [2] of CA cert chain if using private CA for the server cert [1]                                 |
| KAFKA_SEC_SKIP_VERIFY              | `false`                                                                                                                                                                                                   | ignore server certificate issues if set to `true` [1]                                                |
| GENERATE_DOWNLOADS_TOPIC           | `filter-job-submitted`                                                                                                                                                                                    | The topic to send generate full dataset version downloads to                                         |
| HEALTHCHECK_INTERVAL               | `30s`                                                                                                                                                                                                     | The time between calling healthcheck endpoints for check subsystems                                  |
| HEALTHCHECK_CRITICAL_TIMEOUT       | `90s`                                                                                                                                                                                                     | The time taken for the health changes from warning state to critical due to subsystem check failures |
| ENABLE_PRIVATE_ENDPOINTS           | `false`                                                                                                                                                                                                   | Enable private endpoints for the API                                                                 |
| DISABLE_GRAPH_DB_DEPENDENCY        | `false`                                                                                                                                                                                                   | Disables connection and health check for graph db                                                    |
| DOWNLOAD_SERVICE_SECRET_KEY        | `QB0108EZ-825D-412C-9B1D-41EF7747F462`                                                                                                                                                                    | A key specific for the download service to access public/private links                               |
| ZEBEDEE_URL                        | `http://localhost:8082`                                                                                                                                                                                   | The host name for Zebedee                                                                            |
| ENABLE_PERMISSIONS_AUTH            | `false`                                                                                                                                                                                                   | Enable/disable user/service permissions checking for private endpoints                               |
| DEFAULT_MAXIMUM_LIMIT              | `1000`                                                                                                                                                                                                    | Default maximum limit for pagination                                                                 |
| DEFAULT_LIMIT                      | `20`                                                                                                                                                                                                      | Default limit for pagination                                                                         |
| DEFAULT_OFFSET                     | `0`                                                                                                                                                                                                       | Default offset for pagination                                                                        |
|                                    |                                                                                                                                                                                                           |                                                                                                      |
| OTEL_BATCH_TIMEOUT                 | `5s` 
| Interval between pushes to OT Collector                          |
| OTEL_EXPORTER_OTLP_ENDPOINT        | `http://localhost:4317`
| URL for OpenTelemetry endpoint                                   |
| OTEL_SERVICE_NAME                  | `dp-dataset-api`
| Service name to report to telemetry tools                        |
| OTEL_ENABLED                       | `false`
| Feature flag to enable OpenTelemetry                             |

Notes:

1. Ignored unless using TLS (i.e. `KAFKA_SEC_PROTO` has a value enabling TLS)

2. PEM values are identified as those starting with `-----BEGIN`
   and can use `\n` (sic) instead of newlines (they will be converted to newlines before use). Any other value will be
   treated as a path to the given PEM file.

#### Graph / Neptune Configuration

| Environment variable    | Default | Description
| ------------------------| ------- | -----------
| GRAPH_DRIVER_TYPE       | ""      | string identifier for the implementation to be used (e.g. 'neptune' or 'mock')
| GRAPH_ADDR              | ""      | address of the database matching the chosen driver type (web socket)
| NEPTUNE_TLS_SKIP_VERIFY | false   | flag to skip TLS certificate verification, should only be true when run locally

:warning: to connect to a remote Neptune environment on MacOSX using Go 1.18 or higher you must set `NEPTUNE_TLS_SKIP_VERIFY` to true. See our [Neptune guide](https://github.com/ONSdigital/dp/blob/main/guides/NEPTUNE.md) for more details.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2022, [Office for National Statistics](https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details
