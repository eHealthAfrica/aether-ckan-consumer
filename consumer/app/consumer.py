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
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from aet.consumer import BaseConsumer
from aet.logger import get_logger
from aether.python.redis.task import TaskHelper
from conf.consumer import settings

import birdisle
import birdisle.redis

from app import artifacts, config


class CKANConsumer(BaseConsumer):

    def __init__(self, CON_CONF, KAFKA_CONF, redis_instance=None):
        self.job_class = artifacts.CKANJob
        self.redis = redis_instance
        if not self.redis:
            # self.redis = TaskHelper(settings).redis
            password = config.get_consumer_config().get('REDIS_PASSWORD')
            server = birdisle.Server(f'requirepass {password}')
            birdisle.redis.LocalSocketConnection.health_check_interval = 0
            self.redis = birdisle.redis.StrictRedis(server=server, password=password)
            self.redis.config_set('notify-keyspace-events', 'KEA')
        super(CKANConsumer, self).__init__(
            CON_CONF,
            KAFKA_CONF,
            self.job_class,
            redis_instance=self.redis
        )
