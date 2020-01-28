#!/usr/bin/env python

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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

ES_INSTANCE = {
    'id': 'es-test',
    'name': 'Test ES Instance',
    'url': 'http://elasticsearch',
    'user': 'admin',
    'password': 'admin'
}

KIBANA_INSTANCE = {
    'id': 'k-test',
    'name': 'Test Kibana Instance',
    'url': 'http://kibana:5601/kibana-app',
    'user': 'admin',
    'password': 'admin'
}

LOCAL_ES_INSTANCE = {
    'id': 'default',
    'name': 'Test LOCAL ES Instance'
}

LOCAL_KIBANA_INSTANCE = {
    'id': 'default',
    'name': 'Test LOCAL Kibana Instance'
}

SUBSCRIPTION = {
    'id': 'sub-test',
    'name': 'Test Subscription',
    'topic_pattern': '*',
    'topic_options': {
        'masking_annotation': '@aether_masking',  # schema key for mask level of a field
        'masking_levels': ['public', 'private'],  # classifications
        'masking_emit_level': 'public',           # emit from this level ->
        'filter_required': True,                 # filter on a message value?
        'filter_field_path': 'operational_status',    # which field?
        'filter_pass_values': ['operational'],             # what are the passing values?
    },
    'es_options': {
        'alias_name': 'test',
        'auto_timestamp': True,
        'geo_point_creation': True,
        'geo_point_name': 'geopoint'
    },
    'kibana_options': {
        'auto_vizualization': 'schema'  # enum ['full', 'schema', 'none']
    },
    'visualizations': []  # manual visualizations
}

JOB = {
    'id': 'default',
    'name': 'Default ES Consumer Job',
    'local_kibana': 'default',
    'local_elasticsearch': 'default'
}

JOB_FOREIGN = {
    'id': 'j-test-foreign',
    'name': 'Default ES Consumer Job',
    'kibana': 'k-test',
    'elasticsearch': 'es-test',
    'subscription': ['sub-test']
}
