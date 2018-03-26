## Aether CKAN Kafka Consumer

[![Build Status](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer.svg?token=fEtgVpYzTRuyVvasgs5K&branch=master)](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer)

This is an Aether CKAN Kafka Consumer, which consumes datа from Kafka and feeds
CKAN portals with data in the Datastore.

### Running the app

The project is setup with Docker. It will spin up an environment with
Python 2.7 based on Alpine and install the required dependencies.

To run the app just type `docker-compose up`.

### Running the tests

To run the tests, first make sure that you have installed the required
development dependencies, which can be done by running the following command:

```
pip install -r dev-requirements.txt
```

After that just type this command to actually run the tests:

```
nosetests
```

To run the tests and produce a coverage report type this command:

```
nosetests --with-coverage --cover-tests --cover-inclusive --cover-erase
```
