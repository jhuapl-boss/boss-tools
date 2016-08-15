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
# Short-Description: Service to maintain the delayed write daemon on CacheManager
# Description: Service to maintaining Delayed Write Daemon on CacheManager
#
### END INIT INFO

import time
import uuid

from bossutils import daemon_base
from bossutils import configuration

from spdb.spatialdb.spatialdb import SpatialDB
from spdb.project.basicresource import BossResourceBasic
import redis


class DelayedWriteDaemon(daemon_base.DaemonBase):

    def process(self, sp):
        """

        Args:
            sp:

        Returns:

        """
        # Get All delayed writes
        delay_write_keys = sp.cache_state.get_all_delayed_write_keys()

        if delay_write_keys:
            for delay_write_key in delay_write_keys:
                # Get first write-cuboid key
                write_cuboid_key = sp.cache_state.check_single_delayed_write(delay_write_key)
                # If no keys, delete in a transaction
                if not write_cuboid_key:
                    with sp.cache_state.status_client.pipeline() as pipe:
                        try:
                            pipe.watch(delay_write_key)
                            pipe.multi()
                            pipe.delete(delay_write_key)
                            _ = pipe.execute()

                        except redis.WatchError as e:
                            # Watch error occurred, try again!
                            continue

                        except Exception as e:
                            self.log.error(
                                "An error occurred while deleting delayed-write key: {}\n{}".format(delay_write_key,
                                                                                                    e))

                # Check if it is being paged out still
                temp_page_out_key = "TEMP&{}".format(uuid.uuid4().hex)
                _, parts = delay_write_key.split("&", 1)
                lookup, res, time_sample, morton = parts.rsplit("&", 3)
                if sp.cache_state.in_page_out(temp_page_out_key, lookup, res, morton, time_sample):
                    self.log.info("Key in page out. Ignoring")
                    # if so, continue
                    continue

                # If not, get page-out lock
                temp_page_out_key = "TEMP&{}".format(uuid.uuid4().hex)
                in_page_out = sp.cache_state.add_to_page_out(temp_page_out_key, lookup,
                                                                      res, morton, time_sample)

                # Trigger flush lambda, which will collapse all delayed-write keys into a single S3 IO op
                if not in_page_out:
                    write_cuboid_key, resource_str = sp.cache_state.get_single_delayed_write(delay_write_key)
                    self.log.info("Triggering Lambda for: {}".format(write_cuboid_key))
                    resource = BossResourceBasic()
                    resource.from_json(resource_str)
                    sp.objectio.trigger_page_out({"kv_config": sp.kv_config,
                                                  "state_config": sp.state_conf,
                                                  "object_store_config": sp.object_store_config},
                                                 write_cuboid_key,
                                                 resource)

    def run(self):
        # Setup SPDB instance
        config = configuration.BossConfig()
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

        sp = SpatialDB(kvio_config,
                       state_config,
                       object_store_config)

        while True:
            self.log.info("Checking for delayed write operations.")
            self.process(sp)
            time.sleep(10)

if __name__ == '__main__':
    DelayedWriteDaemon("boss-delayedwrited.pid").main()
