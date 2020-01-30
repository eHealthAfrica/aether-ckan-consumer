#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
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

from . import *  # noqa # get all test assets from tests/__init__.py
from unittest import TestCase

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.


class ConsumerTest(TestCase):

    @pytest.mark.unit
    def test__get_config_alias(self):
        ckan_servers = KAFKA_CONFIG.get('bootstrap.servers')
        self.assertIsNotNone(ckan_servers)
        args = KAFKA_CONFIG.copy()
        bootstrap = 'bootstrap.servers'.upper()
        self.assertIn(bootstrap, args)
        self.assertEqual(args.get(bootstrap), os.environ.get('KAFKA_URL'))
        self.assertIsNone(args.get('KAFKA_URL'))
