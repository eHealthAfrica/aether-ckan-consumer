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

avroToPostgresPrimitiveTypes = {
    'string': 'text',
    'boolean': 'bool',
    'int': 'int4',
    'long': 'int8',
    'float': 'float4',
    'double': 'float8',
    'bytes': 'bytea',
}

def extract_fields_from_schema(schema):
    fields = []
    definition_names = []
    if isinstance(schema, list):
        for definition in schema:
            is_base_schema = definition.get('aetherBaseSchema')

            if is_base_schema:
                for field in definition.get('fields'):
                    fields.append({
                        'name': field.get('name'),
                        'type': field.get('type'),
                    })
            else:
                definition_names.append(definition.get('name'))
    else:
        for field in schema.get('fields'):
            fields.append({
                'name': field.get('name'),
                'type': field.get('type'),
            })
    return fields, definition_names

def prepare_fields_for_resource(fields, definition_names):
    resource_fields = []

    for field in fields:
        resource_field_type = None
        if is_field_primitive_type(field):
            resource_field_type = \
                avroToPostgresPrimitiveTypes.get(field.get('type'))
        elif type(field.get('type')) is dict:
            field_type = field.get('type').get('type')

            if field_type == 'record' or field_type == 'map':
                resource_field_type = 'json'
            elif field_type == 'array':
                field_type = field.get('type').get('items')
                resource_field_type = '_{0}'.format(
                    avroToPostgresPrimitiveTypes.get(field_type)
                )
            elif field_type == 'enum':
                resource_field_type = \
                    avroToPostgresPrimitiveTypes.get('string')
        elif type(field.get('type')) is list:
            union_types = field.get('type')

            for union_type in union_types:
                if union_type in definition_names or \
                    (type(union_type) is dict and
                        union_type.get('type') == 'map'):
                    resource_field_type = 'json'
                    break

            if resource_field_type:
                resource_fields.append({
                    'type': resource_field_type,
                    'id': field.get('name'),
                })
                continue

            for union_type in union_types:
                if (type(union_type) is dict and
                        union_type.get('type') == 'array'):
                    field_type = union_type.get('items')
                    if not isinstance(field_type, dict):
                        resource_field_type = '_{0}'.format(
                            avroToPostgresPrimitiveTypes.get(field_type)
                        )
                    else:
                        resource_field_type = 'json'
                    break

            if resource_field_type:
                resource_fields.append({
                    'type': resource_field_type,
                    'id': field.get('name'),
                })
                continue

            if 'bytes' in union_types:
                resource_field_type = \
                    avroToPostgresPrimitiveTypes.get('bytes')
            elif 'string' in union_types:
                resource_field_type = \
                    avroToPostgresPrimitiveTypes.get('string')

            elif isinstance(union_types[1], dict):
                resource_field_type = 'json'
            else:
                resource_field_type = \
                    avroToPostgresPrimitiveTypes.get(union_types[1])

        if resource_field_type:
            resource_fields.append({
                'type': resource_field_type,
                'id': field.get('name'),
            })

    return resource_fields


def is_field_primitive_type(field):
    field_type = field.get('type')
    if isinstance(field_type, str) and field_type in avroToPostgresPrimitiveTypes.keys():
        return True
    return False
