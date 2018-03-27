## Aether CKAN Kafka Consumer

[![Build Status](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer.svg?token=fEtgVpYzTRuyVvasgs5K&branch=master)](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer)

This is an Aether CKAN Kafka Consumer, which consumes datа from Kafka and feeds
CKAN portals with data in the Datastore.

### Running the app

The project is setup with Docker. It will spin up an environment with
Python 2.7 based on Alpine and install the required dependencies.

To run the app just type:

```
docker-compose up
```

### Running the tests

To run the tests type the following command which also checks for PEP8 errors:

```
docker-compose -f docker-compose.test.yml up --build
```
