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

import time

from bossutils import daemon_base
from bossutils.configuration import BossConfig
import boto3


class DeadLetterDaemon(daemon_base.DaemonBase):
    def __init__(self, pid_file_name, pid_dir="/var/run"):
        super().__init__(pid_file_name, pid_dir)
        self.config = BossConfig()
        self.dead_letter_queue = self.config['aws']['s3-flush-deadletter-queue']
        self.sqs_client = boto3.client('sqs')
        self._sp = None

    def set_spatialdb(self, sp):
        """Set the instance of spatialdb to use."""
        self._sp = sp

    def run(self):
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

        while True:
            resp = self.sqs_client.receive_message(
                QueueUrl=self.dead_letter_queue
            )
            print(resp)
            if 'Messages' in resp:
                handle_messages(resp['Messages'])

            time.sleep(30)

    def handle_messages(self, messages):
        if len(messages) < 1:
            return

        for msg in messages:
            if 'Body' not in msg:
                self.log.error('Got message with no body.')
                continue

            body = msg['Body']
            if 'write_cuboid_key' not in body:
                self.log.error('Message did not have write_cuboid_key set.')
                continue

            key = body['write_cuboid_key']
            lookup_key = self.extract_lookup_key(key)

            if self._sp.cache_state.project_locked(lookup_key):
                # Already write-locked, so nothing to do.
                continue

            self._sp.cache_state.set_project_lock(lookup_key, True)

            # Send notification that something is wrong!
            self.send_alert(lookup_key)

    def send_alert(self, lookup_key):
        pass

    def extract_lookup_key(self, write_cuboid_key):
        vals = write_cuboid_key.split("&")
        lookup_key = "{}&{}&{}".format(vals[1], vals[2], vals[3])
        return lookup_key

if __name__ == '__main__':
    DeadLetterDaemon("boss-deadletterd.pid").main()
