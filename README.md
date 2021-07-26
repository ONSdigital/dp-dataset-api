dp-dataset-api
==================
A ONS API used to navigate datasets, editions and versions - which are published.

### Installation

#### Database
* Run `brew install mongo`
* Run `brew services start mongodb`

* Run `brew install neo4j`
* Configure neo4j, edit `/usr/local/Cellar/neo4j/3.2.0/libexec/conf/neo4j.conf`
* Set `dbms.security.auth_enabled=false`
* Run `brew services restart neo4j`

#### Getting started

* Run api auth stub, [see documentation](https://github.com/ONSdigital/dp-auth-api-stub)
* Run `make debug`

### State changes

Normal sequential order of states:

1. `created` (only on *instance*)
2. `submitted` (only on *instance*)
3. `completed` (only on *instance*)
4. `edition-confirmed` (only on *instance* - this will create an *edition* and *version*,
    in other words the *instance* will now be accessible by `version` endpoints).
    Also the dataset `next` sub-document will also get updated here and so will the *edition*
    (authorised users will see a different latest *version* link versus unauthorised users)
5. `associated` (only on *version*) - dataset `next` sub-document will be updated again and so will the *edition*
6. `published` (only on *version*) - both *edition* and *dataset* are updated - must not be changed

There is the possibility to **rollback** from `associated`  to `edition-confirmed`
where a PST user has attached the _version_ to the wrong collection and so not only does
the `collection_id` need to be updated with the new one (or removed altogether)
but the state will need to revert back to `edition-confirmed`.

Lastly, **skipping a state**: it is possibly to jump from `edition-confirmed` to `published`
as long as all the mandatory fields are there. There also might be a scenario whereby
the state can change from `created` to `completed`, missing out the step to `submitted`
due to race conditions, this is not expected to happen,
the path to get to `completed` is longer than the `submitted` one.

### Healthcheck

The endpoint `/health` checks the connection to the database and returns
one of:

* success (200, JSON "status": "OK")
* failure (500, JSON "status": "error").

The `/health` endpoint replaces `/healthcheck`, which now returns a `404 Not Found` response.

### Kafka scripts

Scripts for updating and debugging Kafka can be found [here](https://github.com/ONSdigital/dp-data-tools)(dp-data-tools)

### Configuration

| Environment variable         | Default                                | Description
| ---------------------------- | ---------------------------------------| -----------
| BIND_ADDR                    | :22000                                 | The host and port to bind to
| MONGODB_BIND_ADDR            | localhost:27017                        | The MongoDB bind address
| MONGODB_DATABASE             | datasets                               | The MongoDB dataset database
| MONGODB_COLLECTION           | datasets                               | MongoDB collection
| SECRET_KEY                   | FD0108EA-825D-411C-9B1D-41EF7727F465   | A secret key used authentication
| CODE_LIST_API_URL            | http://localhost:22400                 | The host name for the CodeList API
| DATASET_API_URL              | http://localhost:22000                 | The host name for the Dataset API
| GRACEFUL_SHUTDOWN_TIMEOUT    | 5s                                     | The graceful shutdown timeout in seconds
| WEBSITE_URL                  | http://localhost:20000                 | The host name for the website
| KAFKA_ADDR                   | localhost:9092                         | The list of kafka hosts
| GENERATE_DOWNLOADS_TOPIC     | filter-job-submitted                   | The topic to send generate full dataset version downloads to
| HEALTHCHECK_INTERVAL         | 30s                                    | The time between calling healthcheck endpoints for check subsystems
| HEALTHCHECK_CRITICAL_TIMEOUT | 90s                                    | The time taken for the health changes from warning state to critical due to subsystem check failures
| ENABLE_PRIVATE_ENDPOINTS     | false                                  | Enable private endpoints for the API
| DISABLE_GRAPH_DB_DEPENDENCY  | false                                  | Disables connection and health check for graph db
| DOWNLOAD_SERVICE_SECRET_KEY  | QB0108EZ-825D-412C-9B1D-41EF7747F462   | A key specific for the download service to access public/private links
| ZEBEDEE_URL                  | http://localhost:8082                  | The host name for Zebedee
| ENABLE_PERMISSIONS_AUTH      | false                                  | Enable/disable user/service permissions checking for private endpoints
| DEFAULT_MAXIMUM_LIMIT        | 1000                                   | Default maximum limit for pagination
| DEFAULT_LIMIT                | 20                                     | Default limit for pagination
| DEFAULT_OFFSET               | 0                                      | Default offset for pagination


### Audit vulnerability
The current version of jwt-go (4.0.0-preview1) is a preview release. This is a known vulnerability which has been excluded using the CVE-ID when running the audit command and should be updated when a stable version of the library is released.
### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
