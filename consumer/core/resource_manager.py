import logging
from threading import Thread, Lock
import sys
import json

from ckanapi import RemoteCKAN
from ckanapi import errors as ckanapi_errors

from consumer.core.topic_manager import TopicManager
from consumer.db import Resource, CkanServer

# Lock is created when creating a resource in CKAN, because there is a bug if
# multiple resources are created concurrently. It sets the resource metadata
# field "state" to "deleted". See https://github.com/ckan/ckan/issues/4217
resource_create_lock = Lock()


class ResourceManager(Thread):

    def __init__(self, dataset_manager, config):
        super(ResourceManager, self).__init__()

        self.logger = logging.getLogger(__name__)
        self.config = config
        self.schema = None
        self.dataset_manager = dataset_manager

    def run(self):
        resource = self.config.get('resource')
        dataset_name = self.config.get('dataset').get('metadata').get('name')

        with resource_create_lock:
            self.create_resource_in_ckan(resource, dataset_name)
            self.create_resource_in_datastore()
            self.create_resource_in_db(resource, dataset_name)

            self.spawn_topic_managers()

    def spawn_topic_managers(self):
        dataset = self.config.get('dataset')
        dataset_name = dataset.get('metadata').get('name')
        resource = self.config.get('resource')
        self.topic_managers = []

        topics = resource.get('topics')

        for topic_config in topics:
            number_of_consumers = topic_config.get('number_of_consumers')

            for i in range(number_of_consumers):
                config = {
                    'server_name': self.config.get('server_name'),
                    'dataset_name': dataset_name,
                    'topic': topic_config,
                    'resource_name': resource.get('metadata').get('name')
                }
                topic_manager = TopicManager(self, config)
                self.topic_managers.append(topic_manager)

        if len(self.topic_managers) == 0:
            self.logger.info('No Topic Managers spawned.')
        else:
            self.logger.info(
                'Spawned {0} Topic manager(s) for dataset {1}.'
                .format(len(self.topic_managers), dataset_name)
            )

        for topic_manager in self.topic_managers:
            topic_manager.start()

    def create_resource_in_db(self, resource, dataset_name):
        metadata = resource.get('metadata')
        resource_name = metadata.get('name')
        ckan_server_url = self.config.get('ckan_url')
        ckan_server = CkanServer.get_by_url(
            ckan_server_url=ckan_server_url
        )
        resource = Resource.get(
            resource_name=resource_name,
            ckan_server_id=ckan_server.ckan_server_id,
            dataset_name=dataset_name
        )

        if not resource:
            data = {
                'resource_name': resource_name,
                'dataset_name': dataset_name,
                'ckan_server_id': ckan_server.ckan_server_id,
                'resource_id': self.resource_id
            }

            Resource.create(**data)

    def create_resource_in_ckan(self, resource, dataset_name):
        server_url = self.config.get('ckan_url')
        server_title = self.config.get('server_name')
        api_key = self.config.get('api_key')
        title = resource.get('metadata').get('title')
        resource_name = resource.get('metadata').get('name')
        resource_description = resource.get('metadata').get('description')

        payload = {
            'package_id': dataset_name,
            'name': title,
            'description': resource_description,
            'url_type': 'datastore',
        }

        self.ckan = RemoteCKAN(server_url, apikey=api_key)

        try:
            ckan_server = CkanServer.get_by_url(
                ckan_server_url=server_url
            )
            db_resource = Resource.get(
                resource_name=resource_name,
                dataset_name=dataset_name,
                ckan_server_id=ckan_server.ckan_server_id
            )

            if db_resource:
                self.resource_id = db_resource.resource_id
                return
        except ckanapi_errors.NotFound:
            # Resource does not exist, so continue with execution to create it.
            pass

        try:
            response = self.ckan.action.resource_create(**payload)
            self.resource_id = response.get('id')
            resource_url = '{0}/dataset/{1}/resource/{2}'.format(
                server_url, dataset_name, response.get('id')
            )
            self.logger.info('Resource {0} created in CKAN portal {1}: {2}.'
                             .format(
                                resource_name,
                                server_title,
                                resource_url
                             ))
        except ckanapi_errors.NotAuthorized as e:
            self.logger.error(
                'Cannot create resource {0}. {1}'.format(
                    resource_name,
                    str(e),
                )
            )
            sys.exit(1)
        except ckanapi_errors.ValidationError as e:
            self.logger.error(
                'Cannot create resource {0}. Payload is not valid. Check the '
                'following errors: {1}'.format(
                    resource_name,
                    json.dumps(e.error_dict),
                )
            )
            sys.exit(1)

    def send_data_to_datastore(self, fields, records):
        if not self.schema:
            payload = {
                'id': self.resource_id,
                'limit': 1,
            }

            try:
                response = self.ckan.action.datastore_search(**payload)
            except ckanapi_errors.CKANAPIError:
                self.logger.error(
                    'An error occured while getting Datastore fields for '
                    'resource {0}'
                    .format(self.get_resource_url())
                )
                return

            new_fields = response.get('fields')

            new_fields[:] = [field for field in new_fields if field.get('id') != '_id']

            self.schema = new_fields

        schema_changes = self.get_schema_changes(self.schema, fields)

        if len(self.schema) == 0 or len(schema_changes) > 0:
            for new_field in schema_changes:
                self.schema.append(new_field)

            payload = {
                'resource_id': self.resource_id,
                'fields': self.schema,
            }

            try:
                self.ckan.action.datastore_create(**payload)
            except ckanapi_errors.CKANAPIError:
                self.logger.error(
                    'An error occured while adding new fields for resource {0}'
                    ' in Datastore'
                    .format(self.get_resource_url())
                )

        records = self.convert_string_to_array(records)

        payload = {
            'resource_id': self.resource_id,
            'method': 'insert',
            'records': records,
        }

        try:
            self.ckan.action.datastore_upsert(**payload)
        except ckanapi_errors.CKANAPIError:
            self.logger.error(
                'An error occured while inserting data into resource {0}'
                .format(self.get_resource_url())
            )

    def create_resource_in_datastore(self):
        payload = {
            'resource_id': self.resource_id,
        }

        try:
            self.ckan.action.datastore_create(**payload)
        except ckanapi_errors.CKANAPIError:
            self.logger.error(
                'An error occured while creating resource {0} in Datastore'
                .format(self.get_resource_url())
            )

    def get_schema_changes(self, schema, fields):
        """ Only check if new field has been added. """

        new_fields = []

        for field in fields:
            field_found = False

            for schema_field in schema:
                if field.get('id') == schema_field.get('id'):
                    field_found = True
                    break

            if not field_found:
                new_fields.append(field)

        return new_fields

    def convert_string_to_array(self, records):
        """ If some of fields is of type array, and value for that field
        is a string, then it needs to be converted to an array. """

        array_fields = []
        records = records[:]

        for field in self.schema:
            if field.get('type').startswith('_'):
                array_fields.append(field.get('id'))

        for record in records:
            for key, value in record.items():
                if key in array_fields and type(value) is unicode:
                    record[key] = [value]

        return records

    def stop(self):
        for topic_manager in self.topic_managers:
            topic_manager.stop()

    def on_topic_exit(self, topic_name):
        for topic_manager in self.topic_managers:
            if topic_manager.name == topic_name:
                self.topic_managers.remove(topic_manager)
                break

        if len(self.topic_managers) == 0:
            self.dataset_manager.on_resource_exit(self.name)

    def get_resource_url(self):
        server_url = self.config.get('ckan_url')
        dataset_name = self.config.get('dataset').get('metadata').get('name')
        resource_url = '{0}/dataset/{1}/resource/{2}'.format(
            server_url, dataset_name, self.resource_id
        )

        return resource_url
