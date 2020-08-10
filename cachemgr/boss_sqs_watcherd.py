#!/usr/bin/python3

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
        self.log = logger.bossLogger()

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
                               "s3_index_table": self.config['aws']['s3-index-table'],
                               "id_index_table": self.config['aws']['id-index-table'],
                               "id_count_table": self.config['aws']['id-count-table']}

        config_data = {"kv_config": kvio_config,
                       "state_config": state_config,
                       "object_store_config": object_store_config}
        self.lambda_data = {"config": config_data,
                            "lambda-name": "s3_flush"}

        self.sqs_watcher = SqsWatcher(self.lambda_data)

    def run(self):
        self.sqs_watcher.loop()


class SqsWatcher:
    def __init__(self, lambda_data):
        self.lambda_data = lambda_data
        self.log = logger.bossLogger()
        self.old_message_num = 0
        self.message_num = 0

    def check_queue_count(self, client):
        response = client.get_queue_attributes(
            QueueUrl=self.lambda_data["config"]["object_store_config"]["s3_flush_queue"],
            AttributeNames=[
                'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        https_status_code = response['ResponseMetadata']['HTTPStatusCode']
        if https_status_code == 200:
            queue_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        else:
            queue_count = None
        return https_status_code, queue_count

    def verify_queue(self):
        client = boto3.client('sqs', region_name=get_region())
        https_status_code, new_message_num = self.check_queue_count(client)

        if new_message_num is None:
            self.log.error("sqs_watcherd get_queue_attributes failed. Response HTTPSStatusCode: " + str(
                https_status_code))
            return

        self.old_message_num = self.message_num
        self.message_num = new_message_num

        if ((self.message_num != 0) and (self.message_num == self.old_message_num)):
            client = boto3.client('lambda', region_name=get_region())
            lambdas_to_invoke = min(self.message_num, MAX_LAMBDAS_TO_INVOKE)
            self.log.info("kicking off {} lambdas".format(lambdas_to_invoke))
            for i in range(lambdas_to_invoke):
                response = client.invoke(
                    FunctionName=self.lambda_data["config"]["object_store_config"]["page_out_lambda_function"],
                    InvocationType='Event',
                    Payload=json.dumps(self.lambda_data).encode())
                if response['ResponseMetadata']['HTTPStatusCode'] != 202:
                    self.log.error("sqs_watcherd invoke_lambda failed. Response HTTPSStatusCode: " + str(
                        response['ResponseMetadata']['HTTPStatusCode']))
            return lambdas_to_invoke

    def loop(self):
        while True:
            self.verify_queue()
            time.sleep(15)


if __name__ == '__main__':
    SqsWatcherDaemon("boss-sqs-watcherd.pid").main()
