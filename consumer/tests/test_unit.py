#!/usr/bin/env python

# Copyright (C) 2020 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

import os
import pytest

from . import (
    KAFKA_CONFIG,
    KAFKA_ADMIN_CONFIG,
    CONSUMER_CONFIG,
    CKANConsumer,
    AUTOGEN_SCHEMA,
    TEST_LIST_SIMPLE_SCHEMAS,
    TEST_SIMPLE_SCHEMA,
 ) # noqa # get all test assets from tests/__init__.py

from app.fixtures import examples
from app.utils import (
    extract_fields_from_schema,
    prepare_fields_for_resource,
    is_field_primitive_type,
    avroToPostgresPrimitiveTypes,
)
from app.artifacts import (
    CKANInstance
)

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.

@pytest.mark.unit
def test__get_config_alias():
    ckan_servers = KAFKA_CONFIG.get('bootstrap.servers')
    assert(ckan_servers is not None)
    args = KAFKA_CONFIG.copy()
    bootstrap = 'bootstrap.servers'.upper()
    assert(bootstrap in args)
    assert(args.get(bootstrap) == os.environ.get('KAFKA_URL'))
    assert(args.get('KAFKA_URL') is None)


@pytest.mark.unit
def test__utils():
    fields, definition_names = extract_fields_from_schema(TEST_SIMPLE_SCHEMA)
    assert(len(fields) == 2)
    assert(len(definition_names) == 0)
    resource_types = prepare_fields_for_resource(fields, definition_names)
    for _type in resource_types:
        assert(_type['type'] not in avroToPostgresPrimitiveTypes)

    fields, definition_names = extract_fields_from_schema(TEST_LIST_SIMPLE_SCHEMAS)
    assert(len(fields) == 3)
    assert(len(definition_names) == 1)
    assert('int' in fields[2]['type'])
    resource_types = prepare_fields_for_resource(fields, definition_names)
    for _type in resource_types:
        assert(_type['type'] not in avroToPostgresPrimitiveTypes)

    fields, definition_names = extract_fields_from_schema(AUTOGEN_SCHEMA)
    resource_types = prepare_fields_for_resource(fields, definition_names)
    for _type in resource_types:
        assert(_type['type'] not in avroToPostgresPrimitiveTypes)
