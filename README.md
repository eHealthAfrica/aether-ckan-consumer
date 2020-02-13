## Aether CKAN Kafka Consumer

[![Build Status](https://travis-ci.com/eHealthAfrica/aether-ckan-consumer.svg?token=gHAe1qxjJyFVW9WoFY5S&branch=master)](https://travis-ci.com/eHealthAfrica/aether-ckan-consumer)

This is an Aether CKAN Kafka Consumer, which consumes datа from Kafka and feeds
CKAN portals with data in the Datastore.

### Running the app

A CKAN portal has to be available fro the consumer to connect to. You can fine how to setup a CKAN portal [here](https://docs.ckan.org/en/2.8/maintaining/installing/index.html), or check our example of a docker-compose CKAN container `./consumer/tests/ckan`

After setting up CKAN and a CKAN user, get your [api-key](https://docs.ckan.org/en/ckan-2.7.3/api/#authentication-and-api-keys) for later use.

To start the consumer run

```bash
docker-compose up
```

The consumer api should now be accessible on [http://localhost:9009](http://localhost:9009)

### Configuration

#### `/conf/consumer/kafka.json`

To setup the main consumer, the kafka.json file should match your preferred Kafka settings. This is not user facing. The consumer running assumes that you have employed topic level access control.

You can also set the default masking and message filtering settings here, but if specified, the user's rules will take precedence.

```json
{
    "auto_offset_reset" : "earliest",
    "aether_emit_flag_required" : false,
    "aether_masking_schema_levels" : ["false", "true"],
    "aether_masking_schema_emit_level": "false",
    "heartbeat_interval_ms": 2500,
    "session_timeout_ms": 18000,
    "request_timeout_ms": 20000,
    "consumer_timeout_ms": 17000
}
```

#### `/conf/consumer/consumer.json`

The consumer takes data from kafka and groups them into `datasets` on CKAN. This file defines the defaults for datasets if none is provided by the user during subscription.

```json
{
    "name": "CKAN_CONSUMER",
    "metadata": {
        "author": "eHealth Africa",
        "author_email": "info@ehealthafrica.org",
        "maintainer": "eHealth Africa",
        "maintainer_email": "info@ehealthafrica.org",
        "license_id": "cc-by",
        "url": "https://www.ehealthafrica.org",
        "version": "1.0",
        "owner_org": "eHA",
        "name": "demo-dataset-1",
        "title": "Demo Dataset"
    }
}
```

### Usage

As with all consumers built on the SDK, tasks are driven by a Job which has a set of Resources. In this case, a Job has a `subscription` to a topic (or wildcard) on Kafka, and sends data to a `ckan` instance. All resource examples and schemas can be found in `/consumer/app/fixtures`

Using the consumer API usually on `http://localhost:9009`, register the following artefacts.

#### CKAN

(post to `/ckan/add` as `json`)

```json
{
    "id": "ckan-id",
    "name": "CKAN Instance",
    "url": "http://ckan:5000",
    "key": `[your-ckan-api-key]`
}
```

#### Subscription

(post to `/subscription/add` as `json`)

```json
{
    "id": "sub-id",
    "name": "Demo Subscription",
    "topic_pattern": "*",
    "topic_options": {
        "masking_annotation": "@aether_masking",
        "masking_levels": ["public", "private"],
        "masking_emit_level": "public",
        "filter_required": false
    },
    "target_options": {
        "dataset_metadata": {
            "title": "Pollution in Nigeria",
            "name": "pollution-in-nigeria111",
            "owner_org": "eHA",
            "notes": "Some description",
            "author": "eHealth Africa",
            "private": False
        }
    }
}
```

#### Job

Finally we, tie it together with a Job that references the above artifacts by ID. (post to `/job/add` as `json`)

```json
{
    "id": "job-id",
    "name": "CKAN Consumer Job",
    "ckan": "ckan-id",
    "subscription": ["sub-id"]
}
```

### Environment Variables

The following settings can be changed in `docker-compose.yml > services > ckan-consumer > environment`:
 -  `CONSUMER_NAME`: The name of the consumer
 -  `EXPOSE_PORT`: Port to access consumer API
 -  `ADMIN_USER`: Username for API authentication
 -  `ADMIN_PW`: Password for API authentication

 -  `REDIS_DB`: Redis database name
 -  `REDIS_HOST`: Host to redis instance
 -  `REDIS_PORT`: Redis port
 -  `REDIS_PASSWORD`: Redis password


### Control and Artifact Functions

The Aether Consumer SDK allows exposure of functionality on a per Job or per Resource basis. You can query for a list of available functions on any of the artifacts by hitting its describe endpoint. For example; /job/describe yields:

```json
[
  {
    "doc": "Described the available methods exposed by this resource type",
    "method": "describe",
    "signature": "(*args, **kwargs)"
  },
  {
    "doc": "Returns the schema for instances of this resource",
    "method": "get_schema",
    "signature": "(*args, **kwargs)"
  },
  {
    "doc": "Return a lengthy validations.\n{'valid': True} on success\n{'valid': False, 'validation_errors': [errors...]} on failure",
    "method": "validate_pretty",
    "signature": "(definition, *args, **kwargs)"
  },
  {
    "doc": "Temporarily Pause a job execution.\nWill restart if the system resets. For a longer pause, remove the job via DELETE",
    "method": "pause",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": "Resume the job after pausing it.",
    "method": "resume",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": null,
    "method": "get_status",
    "signature": "(self, *args, **kwargs) -> Union[Dict[str, Any], str]"
  },
  {
    "doc": "A list of the last 100 log entries from this job in format\n[\n    (timestamp, log_level, message),\n    (timestamp, log_level, message),\n    ...\n]",
    "method": "get_logs",
    "signature": "(self, *arg, **kwargs)"
  },
  {
    "doc": "Get a list of topics to which the job can subscribe.\nYou can also use a wildcard at the end of names like:\nName* which would capture both Name1 && Name2, etc",
    "method": "list_topics",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": "A List of topics currently subscribed to by this job",
    "method": "list_subscribed_topics",
    "signature": "(self, *arg, **kwargs)"
  }
]
```

### Running the tests

To run the tests type the following command which also checks for PEP8 errors:

```
./scripts/run_unit_tests.sh
./scripts/run_integration_tests.sh
```
