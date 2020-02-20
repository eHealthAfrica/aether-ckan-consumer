# Copyright (C) 2020 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import fnmatch
import requests
import json
from time import sleep
from typing import (
    Callable,
    List
)
from uuid import uuid4
from ast import literal_eval

from confluent_kafka import KafkaException
from ckanapi import RemoteCKAN
from ckanapi import errors as ckanapi_errors

# Consumer SDK
from aet.exceptions import ConsumerHttpException
from aet.job import BaseJob, JobStatus
from aet.kafka import KafkaConsumer, FilterConfig, MaskConfig
from aet.logger import callback_logger, get_logger
from aet.resource import BaseResource

from app.config import get_kafka_config, get_consumer_config
from app.fixtures import schemas
from app.utils import extract_fields_from_schema, prepare_fields_for_resource


LOG = get_logger('artifacts')
KAFKA_CONFIG = get_kafka_config()
CONSUMER_CONFIG = get_consumer_config()


class CKANInstance(BaseResource):
    schema = schemas.CKAN_INSTANCE
    jobs_path = '$.ckan'
    name = 'ckan'
    public_actions = BaseResource.public_actions + [
        'test_connection'
    ]
    session: requests.Session() = None

    def get_session(self):
        if self.session:
            return self.session
        self.session = requests.Session()
        try:
            self.session.auth = (
                self.definition.user,
                self.definition.password
            )
        except AttributeError:
            pass  # may not need creds
        # add an _id so we can check the instance
        setattr(self.session, 'instance_id', str(uuid4()))
        return self.session

    def request(self, method, url, **kwargs):
        try:
            session = self.get_session()
        except Exception as err:
            raise ConsumerHttpException(str(err), 500)
        full_url = f'{self.definition.url}{url}'
        res = session.request(method, full_url, **kwargs)
        try:
            res.raise_for_status()
        except Exception:
            raise ConsumerHttpException(str(res.content), res.status_code)
        return res

    def test_connection(self, *args, **kwargs) -> bool:
        try:
            res = self.request('get', '/api/action/status_show')
        except requests.exceptions.ConnectionError as her:
            raise ConsumerHttpException(str(her), 500)
        except Exception as err:
            LOG.debug(f'Error testing ckan connection {err}, {type(err)}')
            raise ConsumerHttpException(err, 500)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as her:
            LOG.debug(f'Error testing ckan connection {her}')
            raise ConsumerHttpException(her, her.response.status_code)
        return res.json()


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscription'
    name = 'subscription'

    def _handles_topic(self, topic, tenant):
        topic_str = self.definition.topic_pattern
        # remove tenant information
        no_tenant = topic.lstrip(f'{tenant}.')
        return fnmatch.fnmatch(no_tenant, topic_str)


class CKANJob(BaseJob):
    name = 'job'
    _resources = [CKANInstance, Subscription]
    schema = schemas.CKAN_JOB

    public_actions = BaseJob.public_actions + [
        'get_logs',
        'list_topics',
        'list_subscribed_topics'
    ]
    # publicly available list of topics
    subscribed_topics: dict
    log_stack: list
    log: Callable  # each job instance has it's own log object to keep log_stacks -> user reportable

    consumer: KafkaConsumer = None
    # processing artifacts
    _schemas: dict
    _doc_types: dict
    _routes: dict
    _previous_topics: list
    _ckan: CKANInstance
    _subscriptions: List[Subscription]

    def _setup(self):
        self.subscribed_topics = {}
        self._schemas = {}
        self._topic_fields = {}
        self._subscriptions = []
        self._previous_topics = []
        self.log_stack = []
        self.log = callback_logger('JOB', self.log_stack, 100)
        self.group_name = f'{self.tenant}.{self._id}'
        self.sleep_delay: float = 0.5
        self.report_interval: int = 100
        args = {k.lower(): v for k, v in KAFKA_CONFIG.copy().items()}
        args['group.id'] = self.group_name
        LOG.debug(args)
        self.consumer = KafkaConsumer(**args)
        self.rename_fields = {}
        self.bad_terms = []

    def _job_ckan(self, config=None) -> CKANInstance:
        if config:
            ckan = self.get_resources('ckan', config)
            if not ckan:
                raise ConsumerHttpException('No CKAN instance associated with Job', 400)
            self._ckan = ckan[0]
        return self._ckan

    def _job_subscriptions(self, config=None) -> List[Subscription]:
        if config:
            subs = self.get_resources('subscription', config)
            if not subs:
                raise ConsumerHttpException('No Subscriptions associated with Job', 400)
            self._subscriptions = subs
        return self._subscriptions

    def _job_subscription_for_topic(self, topic):
        return next(iter(
            sorted([
                i for i in self._job_subscriptions()
                if i._handles_topic(topic, self.tenant)
            ])),
            None)

    def _test_connections(self, config):
        self._job_subscriptions(config)
        self._job_ckan(config).test_connection()  # raises CHE
        return True

    def _get_messages(self, config):
        try:
            self.log.debug(f'{self._id} checking configurations...')
            self._test_connections(config)
            subs = self._job_subscriptions()
            self._handle_new_subscriptions(subs)
            self.log.debug(f'Job {self._id} getting messages')
            return self.consumer.poll_and_deserialize(
                timeout=5,
                num_messages=1)  # max
        except ConsumerHttpException as cer:
            # don't fetch messages if we can't post them
            self.log.debug(f'Job not ready: {cer}')
            self.status = JobStatus.RECONFIGURE
            sleep(self.sleep_delay * 10)
            return []
        except Exception as err:
            import traceback
            traceback_str = ''.join(traceback.format_tb(err.__traceback__))
            self.log.critical(f'unhandled error: {str(err)} | {traceback_str}')
            raise err
            sleep(self.sleep_delay)
            return []

    def _handle_new_subscriptions(self, subs):
        old_subs = list(sorted(set(self.subscribed_topics.values())))
        for sub in subs:
            pattern = sub.definition.topic_pattern
            # only allow regex on the end of patterns
            if pattern.endswith('*'):
                self.subscribed_topics[sub.id] = f'^{self.tenant}.{pattern}'
            else:
                self.subscribed_topics[sub.id] = f'{self.tenant}.{pattern}'
        new_subs = list(sorted(set(self.subscribed_topics.values())))
        _diff = list(set(old_subs).symmetric_difference(set(new_subs)))
        if _diff:
            self.log.info(f'{self.tenant} added subs to topics: {_diff}')
            self.consumer.subscribe(new_subs, on_assign=self._on_assign)

    def _handle_messages(self, config, messages):
        self.log.debug(f'{self.group_name} | reading {len(messages)} messages')
        ckan_instance = self._job_ckan(config=config)
        server_url = ckan_instance.definition.get('url')
        api_key = ckan_instance.definition.get('key')
        ckan_remote = RemoteCKAN(server_url, apikey=api_key)
        count = 0
        records = []
        topic = None
        for msg in messages:
            topic = msg.topic
            schema = msg.schema
            if schema != self._schemas.get(topic):
                self.log.info(f'{self._id} Schema change on {topic}')
                self._schemas[topic] = schema
                fields, definition_names = extract_fields_from_schema(schema)
                fields = prepare_fields_for_resource(fields, definition_names)
                self._topic_fields[topic] = fields
            else:
                self.log.debug('Schema unchanged.')
            records.append(msg.value)
            resource = self.submit_artefacts(
                topic,
                schema,
                ckan_remote
            )
            count += 1

        if resource:
            self._create_resource_in_datastore(resource, ckan_remote)
            self.send_data_to_datastore(self._topic_fields[topic], records, resource, ckan_remote)
        self.log.info(f'processed {count} {topic} docs')

    def submit_artefacts(self, topic, schema, ckan_remote):
        subscription = self._job_subscription_for_topic(topic)
        target_options = subscription.definition.get('target_options')
        target_dataset_metadata = CONSUMER_CONFIG.get('metadata', {})
        target_dataset_metadata.update(target_options.get('dataset_metadata'))
        dataset = self._create_dataset_in_ckan(target_dataset_metadata, ckan_remote)
        if dataset:
            resource_name = schema.get('name')
            return self._create_resource_in_ckan(resource_name, dataset, ckan_remote)

    # called when a subscription causes a new
    # assignment to be given to the consumer
    def _on_assign(self, *args, **kwargs):
        assignment = args[1]
        for _part in assignment:
            if _part.topic not in self._previous_topics:
                self.log.info(f'New topic to configure: {_part.topic}')
                self._apply_consumer_filters(_part.topic)
                self._previous_topics.append(_part.topic)

    def _apply_consumer_filters(self, topic):
        self.log.debug(f'{self._id} applying filter for new topic {topic}')
        subscription = self._job_subscription_for_topic(topic)
        if not subscription:
            self.log.error(f'Could not find subscription for topic {topic}')
            return
        try:
            opts = subscription.definition.topic_options
            _flt = opts.get('filter_required', False)
            if _flt:
                _filter_options = {
                    'check_condition_path': opts.get('filter_field_path', ''),
                    'pass_conditions': opts.get('filter_pass_values', []),
                    'requires_approval': _flt
                }
                self.log.info(_filter_options)
                self.consumer.set_topic_filter_config(
                    topic,
                    FilterConfig(**_filter_options)
                )
            mask_annotation = opts.get('masking_annotation', None)
            if mask_annotation:
                _mask_options = {
                    'mask_query': mask_annotation,
                    'mask_levels': opts.get('masking_levels', []),
                    'emit_level': opts.get('masking_emit_level')
                }
                self.log.info(_mask_options)
                self.consumer.set_topic_mask_config(
                    topic,
                    MaskConfig(**_mask_options)
                )
            self.log.info(f'Filters applied for topic {topic}')
        except AttributeError as aer:
            self.log.error(f'No topic options for {subscription.id}| {aer}')

    def _create_dataset_in_ckan(self, dataset, ckan):
        dataset_name = dataset.get('name').lower()
        org_name = dataset.get('owner_org').lower()
        # ckan allows only lower case dataset names
        dataset.update({
            'name': dataset_name,
            'owner_org': org_name
        })

        try:
            ckan.action.organization_show(id=org_name)
        except ckanapi_errors.NotFound:
            self.log.debug(f'Creating {org_name} organization')
            try:
                org = {
                    'name': org_name,
                    'state': 'active',
                }
                ckan.action.organization_create(**org)
                self.log.debug(f'Successfully created {org_name} organization')
            except ckanapi_errors.ValidationError as e:
                self.log.error(f'Cannot create organization {org_name} \
                    because of the following errors: {json.dumps(e.error_dict)}')
                return
        except ckanapi_errors.ValidationError as e:
            self.log.error(
                f'Could not find {org_name} organization. {json.dumps(e.error_dict)}'
            )
            return

        try:
            return ckan.action.package_show(id=dataset_name)
        except ckanapi_errors.NotFound:
            # Dataset does not exist, so continue with execution to create it.
            pass

        try:
            new_dataset = ckan.action.package_create(**dataset)
            self.log.debug(f'Dataset {dataset_name} created in CKAN portal.')
            return new_dataset
        except ckanapi_errors.NotAuthorized as e:
            self.log.error(
                f'Cannot create dataset {dataset_name}. {str(e)}'
            )
        except ckanapi_errors.ValidationError as e:
            self.log.error(
                f'Cannot create dataset {dataset_name}. Payload is not valid. \
                    Check the following errors: {json.dumps(e.error_dict)}'
            )

    def _create_resource_in_ckan(self, resource_name, dataset, ckan):

        try:
            resources = ckan.action.resource_search(query=f'name:{resource_name}')
            # todo: filter resource on dataset too
            if resources['count']:
                return resources['results'][0]
        except Exception:
            pass

        try:
            self.log.debug(f'Creating {resource_name} resource')
            resource = {
                'package_id': dataset.get('name'),
                'name': resource_name,
                'url_type': 'datastore',
            }
            new_resource = ckan.action.resource_create(**resource)
            self.log.debug(f'Successfully created {resource_name} resource')
            return new_resource
        except ckanapi_errors.NotAuthorized as e:
            self.log.error(f'Cannot create resource {resource_name}. {str(e)}')
        except ckanapi_errors.ValidationError as e:
            self.log.error(
                f'Cannot create resource {resource_name}. Payload is not valid. \
                    Check the following errors: {json.dumps(e.error_dict)}'
            )

    def _create_resource_in_datastore(self, resource, ckan):
        payload = {
            'resource_id': resource.get('id'),
        }

        try:
            ckan.action.datastore_create(**payload)
        except ckanapi_errors.CKANAPIError as e:
            self.log.error(
                f'An error occurred while creating resource \
                {resource.get("name")} in Datastore. {str(e)}'
            )

    def send_data_to_datastore(self, fields, records, resource, ckan):
        resource_id = resource.get('id')
        resource_name = resource.get('name')
        payload = {
            'id': resource_id,
            'limit': 1,
        }

        try:
            response = ckan.action.datastore_search(**payload)
        except ckanapi_errors.CKANAPIError as e:
            self.log.error(
                f'An error occurred while getting Datastore fields for resource \
                {resource_id}. {str(e)}'
            )
            return

        new_fields = response.get('fields')
        new_fields[:] = [
            field for field in new_fields if field.get('id') != '_id'
        ]

        schema_changes = self.get_schema_changes(new_fields, fields)

        if len(new_fields) == 0 or len(schema_changes) > 0:
            self.log.info('Datastore detected schema changes')
            for new_field in schema_changes:
                new_fields.append(new_field)

            payload = {
                'resource_id': resource_id,
                'fields': new_fields,
            }

            try:
                ckan.action.datastore_create(**payload)
            except ckanapi_errors.CKANAPIError as cke:
                self.log.error(
                    f'An error occurred while adding new fields for resource \
                    {resource_name} in Datastore.'
                )
                label = str(cke)
                self.log.error(
                    'ResourceType: {0} Error: {1}'
                    .format(resource_name, label)
                )
                bad_fields = literal_eval(label).get('fields', None)
                if not isinstance(bad_fields, list):
                    raise ValueError('Bad field could not be identified.')
                issue = bad_fields[0]
                bad_term = str(issue.split(' ')[0]).strip("'").strip('"')
                self.bad_terms.append(bad_term)
                self.log.info(
                    'Recovery from error: bad field name %s' % bad_term)
                self.log.info('Reverting %s' % (schema_changes,))
                for new_field in schema_changes:
                    new_fields.remove(new_field)
                return self.send_data_to_datastore(fields, records, resource, ckan)

        records = self.convert_item_to_array(records, new_fields)

        payload = {
            'resource_id': resource_id,
            'method': 'insert',
            'records': records,
        }

        try:
            ckan.action.datastore_upsert(**payload)
            self.log.info(f'Updated resource {resource_id} in {ckan.address}.')
        except ckanapi_errors.CKANAPIError as cke:
            self.log.error(
                f'An error occurred while inserting data into resource {resource_name}'
            )
            self.log.error(
                f'ResourceType: {resource} Error: {str(cke)}'
            )

    def get_schema_changes(self, schema, fields):
        ''' Only check if new field has been added. '''

        new_fields = []

        for field in fields:
            field_found = False

            for schema_field in schema:
                if field.get('id') == schema_field.get('id'):
                    field_found = True
                    break

            if not field_found:
                if field.get('id') in self.bad_terms:
                    new_fields.append(self.rename_field(field))
                else:
                    new_fields.append(field)

        return new_fields

    def rename_field(self, field):
        bad_name = field.get('id')
        new_name = 'ae' + bad_name
        self.rename_fields[bad_name] = new_name
        field['id'] = new_name
        return field

    def convert_item_to_array(self, records, new_fields):
        ''' If a field is of type array, and the value for it contains a
        primitive type, then convert it to an array of that primitive type.

        This mutation is required for all records, otherwise CKAN will raise
        an exception.

        Example:
            For given field which is of type array of integers
            {'type': '_int', 'id': 'scores'}
            Original record {'scores': 10}
            Changed record {'scores': [10]}
        '''

        array_fields = []
        records = records[:]

        for field in new_fields:
            if field.get('type').startswith('_'):
                array_fields.append(field.get('id'))

        for record in records:
            for key, value in record.items():
                if self.bad_terms:
                    name = self.rename_fields.get(key, key)
                    if name != key:
                        del record[key]
                else:
                    name = key
                if key in array_fields:
                    record[name] = [value]
                else:
                    record[name] = value

        return records

    # public
    def list_topics(self, *args, **kwargs):
        '''
        Get a list of topics to which the job can subscribe.
        You can also use a wildcard at the end of names like:
        Name* which would capture both Name1 && Name2, etc
        '''
        timeout = 5
        try:
            md = self.consumer.list_topics(timeout=timeout)
        except (KafkaException) as ker:
            raise ConsumerHttpException(str(ker) + f'@timeout: {timeout}', 500)
        topics = [
            str(t).split(f'{self.tenant}.')[1]
            for t in iter(md.topics.values())
            if str(t).startswith(self.tenant)
        ]
        return topics

    # public
    def list_subscribed_topics(self, *arg, **kwargs):
        '''
        A List of topics currently subscribed to by this job
        '''
        return list(self.subscribed_topics.values())

    # public
    def get_logs(self, *arg, **kwargs):
        '''
        A list of the last 100 log entries from this job in format
        [
            (timestamp, log_level, message),
            (timestamp, log_level, message),
            ...
        ]
        '''
        return self.log_stack[:]
