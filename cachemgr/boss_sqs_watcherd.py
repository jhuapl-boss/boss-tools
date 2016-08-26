#!/usr/local/bin/python3

# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### BEGIN INIT INFO
# Provides: credentials
# Required-Start:
# Required-Stop:
# Default-Start: 2 3 4 5
# Default-Stop:
# Short-Description: Service for the Prefetch Daemon on CacheManager
# Description: Service for the Prefetch Daemon on CacheManager
#
### END INIT INFO

import boto3
import time
import json

from bossutils import configuration
from bossutils import daemon_base
from bossutils.aws import get_region



class SqsWatcherDaemon(daemon_base.DaemonBase):

    def initialize(self):
        self.config = configuration.BossConfig()

        # kvio settings
        kvio_config = {"cache_host": self.config['aws']['cache'],
                       "cache_db": self.config['aws']['cache-db'],
                       "read_timeout": 86400}

        # state settings
        state_config = {"cache_state_host": self.config['aws']['cache-state'],
                        "cache_state_db": self.config['aws']['cache-state-db']}
        # object store settings
        _, domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        s3_flush_queue_name = "S3FlushQueue.{}".format(domain).replace('.', '-')
        object_store_config = {"s3_flush_queue": self.config["aws"]["s3-flush-queue"],
                               "cuboid_bucket": self.config['aws']['cuboid_bucket'],
                               "page_in_lambda_function": self.config['lambda']['page_in_function'],
                               "page_out_lambda_function": self.config['lambda']['flush_function'],
                               "s3_index_table": self.config['aws']['s3-index-table']}

        config_data = {"kv_config": kvio_config,
                       "state_config": state_config,
                       "object_store_config": object_store_config}

        lambda_data = {"config": config_data,
                       "lambda-name": "s3_flush"}

    def run(self):
        self.initialize()

        while True:
            time.sleep(15)
            client = boto3.client('sqs', region_name="us-east-1")
            response = client.get_queue_attributes(
                QueueUrl=self.config["aws"]["s3-flush-queue"],
                AttributeNames=[
                    'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            old_message_num = message_num;
            message_num = int(response['ApproximateNumberOfMessages'])
            self.log.info("boss-sqs-watcherd: checking sqs queue current messages: {}  prvious messages: {}".format(
                message_num, old_message_num))

            if ((message_num != 0) and (message_num == old_message_num)):
                client = boto3.client('lambda', region_name=get_region())
                self.log.info("kicking off lambda")
                response = client.invoke(
                    FunctionName=self.config["lambda"]["flush_function"],
                    InvocationType='Event',
                    Payload=json.dumps(self.lambda_data).encode())

            self.log.info("sqs_watcherd lambda response: " + str(response))

if __name__ == '__main__':
    SqsWatcherDaemon("boss-sqs-watcherd.pid").main()