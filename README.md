## Aether CKAN Kafka Consumer

[![Build Status](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer.svg?token=fEtgVpYzTRuyVvasgs5K&branch=master)](https://travis-ci.com/ViderumGlobal/aether-ckan-consumer)

This is an Aether CKAN Kafka Consumer, which consumes datа from Kafka and feeds
CKAN portals with data in the Datastore.

### Running the app

The project is setup with Docker. It will spin up an environment with
Python 2.7 based on Alpine and install the required dependencies.

To run the app just type:

```
docker-compose up --timeout 60
```

The `--timeout` argument tells Docker how many seconds to wait before it kills
the app when it is gracefully stopped. The default is 10 seconds. This should
be specified based on how many CKAN portals are configured, and how many
datasets need to be feed with data. Because when the app is gracefully stopped,
 it will send all data processed from Kafka.

### Configuration

#### `config.json`

The Consumer can be configured via `config.json`. This is a sample of its shape
and data:

```json
{
    "database": {
        "url": "sqlite:////srv/app/db/consumer.db"
    },
    "kafka": {
        "url": "localhost:9092"
    },
    "ckan_servers": [
        {
            "title": "Local CKAN portal",
            "url": "http://localhost:5000",
            "api_key": "2ef3752c-b615-405d-9627-2bf7321d4rty",
            "datasets": [
                {
                    "metadata": {
                        "title": "Dataset title",
                        "name": "dataset-title",
                        "owner_org": "demo-org",
                        "notes": "Sample data"
                    },
                    "resources": [
                        {
                            "metadata": {
                                "title": "Sensor data",
                                "description": "Sensor data from wind turbines",
                                "name": "sensor-data"
                            },
                            "topics": [
                                {
                                    "name": "test",
                                    "number_of_consumers": 1
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}
```

Available options are:

- `database`: Object storing information for local database.
- `database.url`: URL where the database is stored.
- `kafka`: Object storing information for Kafka.
- `kafka.url`: The URL where Kafka is running.
- `ckan_servers`: Array of CKAN server instances.
- `ckan_servers.title`: Title of the CKAN server instance.
- `ckan_servers.url`: The URL where the CKAN server instance is running.
- `ckan_servers.api_key`: The API key used for making API calls to a CKAN
server instance. The API key should be associated with a user that has
privileges to create a dataset in CKAN.
- `ckan_servers.datasets`: Array of datasets to feed data with for the CKAN
server instance.
- `ckan_servers.datasets.metadata`: Metadata information for the dataset that
will be created in CKAN.
- `ckan_servers.datasets.metadata.title`: Title of the dataset.
- `ckan_servers.datasets.metadata.name`: Name of the dataset. Create a unique
name for the dataset. The name should be unique per CKAN instance. For
instance, if you want to give the name *demo-dataset* for the dataset, check
the portal at `https://example.com/dataset/demo-dataset` to see if one already
exists. Also, keep in mind that datasets you don't have access to (private),
will be shown as "Not found" on the CKAN portal. So make sure the name is
trully unique for your purpose.
- `ckan_servers.datasets.metadata.owner_org`: Name of the organization. For
instance, for this organization `https://example.com/organization/org-name`,
the name of the organization is *org-name*. This name is unique per CKAN
instance. Note that this organization must be previously created in CKAN from
the UI.
- `ckan_servers.datasets.metadata.notes`: Description of the dataset.
- `ckan_servers.datasets.resources`: List of resources that should be feed with
data.
- `ckan_servers.datasets.resources.metadata`: Metadata information for the
resource in CKAN.
- `ckan_servers.datasets.resources.metadata.title`: Title of the resource.
- `ckan_servers.datasets.resources.metadata.description`: Description of the
resource.
- `ckan_servers.datasets.resources.metadata.name`: Unique name of the resource.
This name should be unique only for the dataset where it is specified, meaning
the same name can be used when used in other dataset.
- `ckan_servers.datasets.resources.topics`: Array of topics to pull data from
for a dataset.
- `ckan_servers.datasets.resources.topics.name`: Name of the topic in Kafka.
- `ckan_servers.datasets.resources.topics.number_of_consumers`: Number of
consumers to instantiate for the specified topic. Usually this should be set to
 1, but if the volume of data that comes from a topic increases, it should be
increased. (Default: 1)

This configuration file is validated against `config.schema`, which is a JSON
Schema file. This makes sure that data in the `config.json` file is valid, as
well as its shape.

#### `dataset_metadata.json`

All datasets specified in `config.json` are created in CKAN with default
metadata fields obtainted from `dataset_metadata.json`. They can be overriden
per dataset through the `config.json` file, in the
`ckan_servers.datasets.resources.metadata` object.

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
