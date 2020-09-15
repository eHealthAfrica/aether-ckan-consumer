#!/usr/bin/env python

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
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import pytest
import requests
import json
from time import sleep

from . import *  # noqa
from . import (  # noqa  # for the linter
    CKANConsumer,
    RequestClientT1,
    RequestClientT2,
    URL
)

from aet.logger import get_logger
from app.fixtures import examples

LOG = get_logger('TEST')


'''
    API Tests
'''


@pytest.mark.unit
def test__consumer_add_delete_respect_tenants(CKANConsumer, RequestClientT1, RequestClientT2):
    res = RequestClientT1.post(f'{URL}/ckan/add', json=examples.CKAN_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/ckan/list')
    assert(res.json() != [])
    res = RequestClientT2.get(f'{URL}/ckan/list')
    assert(res.json() == [])
    res = RequestClientT1.delete(f'{URL}/ckan/delete?id=ckan-id')
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/ckan/list')
    assert(res.json() == [])


@pytest.mark.parametrize('example,endpoint', [
    (examples.CKAN_INSTANCE, 'ckan'),
    (examples.JOB, 'job')
])
@pytest.mark.unit
def test__api_validate(CKANConsumer, RequestClientT1, example, endpoint):
    res = RequestClientT1.post(f'{URL}/{endpoint}/validate', json=example)
    assert(res.json().get('valid') is True), str(res.text)


@pytest.mark.parametrize('example,endpoint', [
    (examples.CKAN_INSTANCE, 'ckan'),
    (examples.JOB, 'job')
])
@pytest.mark.unit
def test__api_validate_pretty(CKANConsumer, RequestClientT1, example, endpoint):
    res = RequestClientT1.post(f'{URL}/{endpoint}/validate_pretty', json=example)
    assert(res.json().get('valid') is True), str(res.text)


@pytest.mark.parametrize('endpoint', [
    ('ckan'),
    ('job')
])
@pytest.mark.unit
def test__api_describe_assets(CKANConsumer, RequestClientT1, endpoint):
    res = RequestClientT1.get(f'{URL}/{endpoint}/describe')
    assert(res.json() is not None), str(res.text)


@pytest.mark.parametrize('endpoint', [
    ('ckan'),
    ('job')
])
@pytest.mark.unit
def test__api_get_schema(CKANConsumer, RequestClientT1, endpoint):
    res = RequestClientT1.get(f'{URL}/{endpoint}/get_schema')
    assert(res.json() is not None), str(res.text)


@pytest.mark.unit
def test__api_resource_instance(CKANConsumer, RequestClientT1):
    doc_id = examples.CKAN_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/ckan/add', json=examples.CKAN_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/ckan/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/ckan/test_connection?id={doc_id}')
    assert(res.json().get('success') is True)

    res = RequestClientT1.delete(f'{URL}/ckan/delete?id={examples.CKAN_INSTANCE.get("id")}')
    assert(res.json() is True)


@pytest.mark.integration
def test__api_resource_ckan(CKANConsumer, RequestClientT1):
    doc_id = examples.CKAN_INSTANCE.get('id')
    res = RequestClientT1.post(f'{URL}/ckan/add', json=examples.CKAN_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/ckan/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/ckan/test_connection?id={doc_id}')
    res.raise_for_status()
    assert(res.json().get('success') is True)


@pytest.mark.integration
def test__api_job_and_resource_create(CKANConsumer, RequestClientT1):
    doc_id = examples.JOB.get('id')
    res = RequestClientT1.post(f'{URL}/ckan/add', json=examples.CKAN_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.post(f'{URL}/subscription/add', json=examples.SUBSCRIPTION)
    assert(res.json() is True)

    res = RequestClientT1.post(f'{URL}/job/add', json=examples.JOB)
    assert(res.json() is True)

    sleep(.25)  # take a few MS for the job to be started


@pytest.mark.integration
def test__api_job_and_resource_public_endpoints(CKANConsumer, RequestClientT1):
    doc_id = examples.JOB.get('id')
    res = RequestClientT1.get(f'{URL}/job/list_subscribed_topics?id={doc_id}')
    res.raise_for_status()
    topics = res.json()
    LOG.debug(topics)
    assert(TEST_TOPIC not in topics)
    sleep(60)
    res = RequestClientT1.get(f'{URL}/job/get_logs?id={doc_id}')
    res.raise_for_status()
    logs = res.json()
    assert(len(logs) > 0)


@pytest.mark.integration
def test__api_job_and_resource_delete(CKANConsumer, RequestClientT1):
    doc_id = examples.JOB.get('id')
    res = RequestClientT1.delete(f'{URL}/ckan/delete?id={examples.CKAN_INSTANCE.get("id")}')
    assert(res.json() is True)
    res = RequestClientT1.post(f'{URL}/subscription/delete?id={examples.SUBSCRIPTION.get("id")}')
    assert(res.json() is True)

    res = RequestClientT1.post(f'{URL}/job/delete?id={doc_id}', json=examples.JOB)
    assert(res.json() is True)
