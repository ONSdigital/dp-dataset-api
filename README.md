dp-dataset-api
==================
A ONS API used to navigate datasets which are published.

#### Postgres
* Run ```brew install mongo```
* Run ```sudo mkdir -p /data/db```
* Run ```sudo chmod 777 /data/db```
* Run ```mongod &```

### Configuration

| Environment variable       | Default                              | Description
| -------------------------- | -------------------------------------| -----------
| BIND_ADDR                  | :22000                               | The host and port to bind to
| MONGODB_BIND_ADDR          | localhost:27017                      | The MongoDB bind address
| MONGODB_DATABASE           | datasets                             | The MongoDB dataset database
| MONGODB_COLLECTION         | datasets                             | MongoDB collection
| SECRET_KEY                 | FD0108EA-825D-411C-9B1D-41EF7727F465 | A secret key used authentication

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details
