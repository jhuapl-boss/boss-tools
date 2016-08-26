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

import time

from bossutils import daemon_base
from bossutils.configuration import BossConfig
from spdb.spatialdb import SpatialDB
from spdb.c_lib import ndlib


class PrefetchDaemon(daemon_base.DaemonBase):

    def __init__(self, pid_file_name, pid_dir="/var/run"):
        super().__init__(pid_file_name, pid_dir)
        self.config = BossConfig()
        self._sp = None

    def set_spatialdb(self, sp):
        """Set the instance of spatialdb to use."""
        self._sp = sp

    def run(self):
        """Main loop."""
        self.configure()

        while True:
            self.process()
            time.sleep(1)

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

    def process(self):
        """Check for cuboid keys in PRE-FETCH.
        """
        obj_key = self.get_object_key()
        if obj_key is None:
            return

        trigger_page_in_lambda(obj_key)

    def get_object_key(self):
        """Get next object-cuboid key in the PRE-FETCH list.

        Returns:
            (string|None): None if the list is empty.
        """
        _bytes = self._sp.cache_state.status_client.lpop('PRE-FETCH')
        if _bytes is None:
            return None
        return str(_bytes, 'utf-8')

    def trigger_page_in_lambda(self, obj_key):
        cached_keys = self._sp.objectio.object_to_cached_cuboid_keys([obj_key])
        # No page in channel created for prefetching.
        page_in_chan = None
        self._sp.objectio.page_in_objects(
            cached_keys, page_in_chan, self._sp.kv_config, self._sp.state_conf)


if __name__ == '__main__':
    PrefetchDaemon("boss-prefetchd.pid").main()
