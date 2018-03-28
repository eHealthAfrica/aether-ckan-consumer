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

### Configuration

The Consumer can be configured via `config.json`. This is a sample of its shape
and data:

```json
{
    "kafka": {
        "url": "kafka:23897"
    },
    "ckan_servers": [
        {
            "name": "CKAN Demo portal",
            "url": "https://example.com",
            "api_key": "a526aab5-9655-4d75-b046-dfd0d060gtyu",
            "datasets": [
                {
                    "name": "some-dataset",
                    "topics": [
                        {
                            "name": "some-topic",
                            "number_of_consumers": 2
                        }
                    ]
                }
            ]
        }
    ]
}
```

Available options are:

- `kafka`: Object storing information for Kafka.
- `kafka.url`: The URL where Kafka is running.
- `ckan_servers`: Array of CKAN server instances.
- `ckan_servers.name`: Name of the CKAN server instance, usually a title.
- `ckan_servers.url`: The URL where the CKAN server instance is running.
- `ckan_servers.api_key`: The API key used for making API calls to a CKAN
server instance.
- `ckan_servers.datasets`: Array of datasets to feed data with for the CKAN
server instance.
- `ckan_servers.datasets.name`: Name of the dataset. For instance, for this
dataset https://example.com/dataset/dataset-name, the name of the dataset is
*dataset-name*.
- `ckan_servers.datasets.topics`: Array of topics to poll data from for a
dataset.
- `ckan_servers.datasets.topics.name`: Name of the topic in Kafka.
- `ckan_servers.datasets.topics.number_of_consumers`: Number of consumers to
instantiate for the specified topic. Usually this should be set to 1, but if
the volume of data that comes from a topic increases, it should be bumped.
(Default: 1)

This configuration file is validated against `config.schema`, which is a JSON
Schema file. This makes sure that data in the `config.json` file is valid, as
well as its shape.

### Environment variables

In `.env`, the following variables can be changed:

- `PYTHONUNBUFFERED`: Useful for debugging in development. It forces stdin,
stdout and stderr to be totally unbuffered. It should be set to any value in
order to work.
- `ENVIRONMENT`: Can be set to *development* or *production*. Default is
*development*.

### Running the tests

To run the tests type the following command which also checks for PEP8 errors:

```
docker-compose -f docker-compose.test.yml up --build
```
