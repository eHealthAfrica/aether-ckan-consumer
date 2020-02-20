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

CKAN_INSTANCE = {
    'id': 'ckan-id',
    'name': 'Test CKAN Instance',
    'url': 'http://ckan:5000',
    'key': 'ckan_api_key'
}

SUBSCRIPTION = {
    'id': 'sub-id',
    'name': 'Test Subscription',
    'topic_pattern': '*',
    'topic_options': {
        'masking_annotation': '@aether_masking',    # schema key for mask level of a field
        'masking_levels': ['public', 'private'],    # classifications
        'masking_emit_level': 'public',             # emit from this level ->
        'filter_required': True,                    # filter on a message value?
        'filter_field_path': 'operational_status',  # which field?
        'filter_pass_values': ['operational'],      # what are the passing values?
    },
    'target_options': {
        'dataset_metadata': {
            'title': 'Pollution in Nigeria',
            'name': 'pollution-in-nigeria111',
            'owner_org': 'eHA ',
            'notes': 'Some description',
            'author': 'eHealth Africa',
            'private': False
        }
    }
}

JOB = {
    'id': 'job-id',
    'name': 'CKAN Consumer Job',
    'ckan': 'ckan-id',
    'subscription': ['sub-id']
}
