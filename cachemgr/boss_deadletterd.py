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
# Short-Description: Service for the Dead Letter Daemon on CacheManager
# Description: Service for the Dead Letter Daemon on CacheManager
#
### END INIT INFO

import json
import time

from bossutils import daemon_base
from bossutils.aws import get_region
from bossutils.configuration import BossConfig
from spdb.spatialdb import SpatialDB
import boto3


class DeadLetterDaemon(daemon_base.DaemonBase):
    def __init__(self, pid_file_name, pid_dir="/var/run"):
        super().__init__(pid_file_name, pid_dir)
        self.config = BossConfig()
        self.dead_letter_queue = self.config['aws']['s3-flush-deadletter-queue']
        self.sns_write_locked = self.config['aws']['sns-write-locked']
        self.sqs_client = boto3.client('sqs', region_name=get_region())
        self.sns_client = boto3.client('sns', region_name=get_region())
        self._sp = None

    def set_spatialdb(self, sp):
        """Set the instance of spatialdb to use."""
        self._sp = sp

    def run(self):
        """Main loop."""
        self.configure()

        while True:
            self.check_queue()
            time.sleep(5)

    def configure(self):
        """Configure spdb instance."""
        config = self.config

        kvio_config = {"cache_host": config['aws']['cache'],
                       "cache_db": config['aws']['cache-state-db'],
                       "read_timeout": 86400}

        state_config = {"cache_state_host": config['aws']['cache-state'],
                        "cache_state_db": config['aws']['cache-db']}

        object_store_config = {"s3_flush_queue": config['aws']["s3-flush-queue"],
                               "cuboid_bucket": config['aws']['cuboid_bucket'],
                               "page_in_lambda_function": config['lambda']['page_in_function'],
                               "page_out_lambda_function": config['lambda']['flush_function'],
                               "s3_index_table": config['aws']['s3-index-table']}

        sp = SpatialDB(kvio_config, state_config, object_store_config)
        self.set_spatialdb(sp)

    def check_queue(self):
        """Check SQS queue and process the message, if any.

        Returns:
            (bool): True if a message was processed.
        """
        resp = self.sqs_client.receive_message(
            QueueUrl=self.dead_letter_queue
        )
        if 'Messages' in resp:
            self.handle_messages(resp['Messages'])
            return True

        return False

    def handle_messages(self, messages):
        """Handler for receiving messages from the dead letter queue.

        Args:
            messages (list): List of messages.  The message should be a failed message from the S3 flush queue.
        """
        if len(messages) < 1:
            return

        for msg in messages:
            if 'ReceiptHandle' in msg:
                # Dequeue message so it won't be processed again.
                self.remove_message_from_queue(msg['ReceiptHandle'])

            if 'Body' not in msg:
                self.log.error('Got message with no body.')
                continue

            body = json.loads(msg['Body'])
            if 'write_cuboid_key' not in body:
                self.log.error('Message did not have write_cuboid_key set.')
                continue

            key = body['write_cuboid_key']
            lookup_key = self.extract_lookup_key(key)

            if self._sp.cache_state.project_locked(lookup_key):
                # Already write-locked, so nothing to do.
                continue

            info = ''
            if 'resource' in body:
                resource = body['resource']
                coll = resource['collection']
                exp = resource['experiment']
                chan_lyr = resource['channel_layer']
                info = 'collection: {}, experiment: {}, channel/layer: {}'.format(coll, exp, chan_lyr)

            self._sp.cache_state.set_project_lock(lookup_key, True)
            self.log.info(
                'Setting write lock for lookup key: {} for {}'.format(lookup_key, info))

            # Send notification that something is wrong!
            self.send_alert(lookup_key, info)

    def send_alert(self, lookup_key, info=None):
        """Publish an alert indicating that a lookup key has been write locked.

        Args:
            lookup_key (string): Key that was locked.
            info (optional[string]): Project info (collection, experiment, etc).
        """
        if info is None:
            info = ''

        self.sns_client.publish(
            TopicArn=self.sns_write_locked,
            Subject='S3 Write-Locked!',
            Message='Error writing to S3.  This lookup key was just locked: {}.  Was trying to write cuboid to: {}'.format(lookup_key, info)
        )

    def remove_message_from_queue(self, receipt_handle):
        """Dequeue the message received from SQS.

        Args:
            receipt_handle (string): This comes from the message response and identifies the message to remove.
        """
        self.sqs_client.delete_message(
            QueueUrl=self.dead_letter_queue,
            ReceiptHandle=receipt_handle
        )

    def extract_lookup_key(self, write_cuboid_key):
        """Extract the lookup key from the cuboid key.

        Args:
            write_cuboid_key (string): Key that failed to write.

        Returns:
            (string)
        """
        vals = write_cuboid_key.split("&")
        lookup_key = "{}&{}&{}".format(vals[1], vals[2], vals[3])
        return lookup_key

if __name__ == '__main__':
    DeadLetterDaemon("boss-deadletterd.pid").main()
