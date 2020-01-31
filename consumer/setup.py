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

from setuptools import setup
setup(
    name='aether_ckan_consumer',
    author='eHealth Africa',
    author_email='aether@ehealthafrica.org',
    decription='Aether CKAN Kafka consumer',
    version='2.0.0',
    install_requires=[
        'aet.consumer>=3.4.1',
        'aether-python',
        'ckanapi',
        'deepmerge',
        'eha_jsonpath',
        'jsonschema',
        'requests',
        'responses',
    ],
    tests_require=['pytest', 'pytest-cov'],
    url='https://github.com/eHealthAfrica/aether-ckan-consumer',
    keywords=['aet', 'aether', 'kafka', 'consumer', 'ckan'],
    classifiers=[]
)
