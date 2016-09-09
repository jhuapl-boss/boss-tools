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
from bossutils import logger
from bossutils.aws import get_region

"""
Daemon to monitor the S3_Flush SQS Queue.  If the ApproximateNumberOfMessages is not zero and is not changing, this
daemon will invoke new lambdas to return the ApproximateNumberOfMessages back to zero.
"""

MAX_LAMBDAS_TO_INVOKE = 10

class SqsWatcherDaemon(daemon_base.DaemonBase):

    def __init__(self, pid_file_name, pid_dir="/var/run"):
        super().__init__(pid_file_name, pid_dir)
        self.log = logger.BossLogger().logger
        self.config = configuration.BossConfig()

        # kvio settings
        kvio_config = {"cache_host": self.config['aws']['cache'],
                       "cache_db": self.config['aws']['cache-db'],
                       "read_timeout": 86400}

        # state settings
        state_config = {"cache_state_host": self.config['aws']['cache-state'],
                        "cache_state_db": self.config['aws']['cache-state-db']}

        # object store settings
        object_store_config = {"s3_flush_queue": self.config["aws"]["s3-flush-queue"],
                               "cuboid_bucket": self.config['aws']['cuboid_bucket'],
                               "page_in_lambda_function": self.config['lambda']['page_in_function'],
                               "page_out_lambda_function": self.config['lambda']['flush_function'],
                               "s3_index_table": self.config['aws']['s3-index-table']}

        config_data = {"kv_config": kvio_config,
                       "state_config": state_config,
                       "object_store_config": object_store_config}

        self.lambda_data = {"config": config_data,
                            "lambda-name": "s3_flush"}

    def run(self):
        old_message_num = 0
        message_num = 0


        while True:
            client = boto3.client('sqs', region_name=get_region())
            response = client.get_queue_attributes(
                QueueUrl=self.config["aws"]["s3-flush-queue"],
                AttributeNames=[
                    'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                self.log.error("sqs_watcherd get_queue_attributes failed. Response HTTPSStatusCode: " + str(
                    response['ResponseMetadata']['HTTPStatusCode']))
                time.sleep(15)
                continue

            old_message_num = message_num
            message_num = int(response['Attributes']['ApproximateNumberOfMessages'])
            self.log.debug("boss-sqs-watcherd: checking sqs queue current messages: {}  previous messages: {}".format(
                message_num, old_message_num))

            if ((message_num != 0) and (message_num == old_message_num)):
                client = boto3.client('lambda', region_name=get_region())
                self.log.info("kicking off lambda")
                lambdas_to_invoke = min(message_num, MAX_LAMBDAS_TO_INVOKE)
                for i in range(lambdas_to_invoke):
                    response = client.invoke(
                        FunctionName=self.config["lambda"]["flush_function"],
                        InvocationType='Event',
                        Payload=json.dumps(self.lambda_data).encode())
                    if response['ResponseMetadata']['HTTPStatusCode'] != 202:
                        self.log.error("sqs_watcherd invoke_lambda failed. Response HTTPSStatusCode: " + str(response['ResponseMetadata']['HTTPStatusCode']))
            time.sleep(15)


if __name__ == '__main__':
    SqsWatcherDaemon("boss-sqs-watcherd.pid").main()