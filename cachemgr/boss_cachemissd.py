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
# Short-Description: Service for the Cache Miss Daemon on CacheManager
# Description: Service for the Cache Miss Daemon on CacheManager
#
### END INIT INFO

import time

from bossutils import daemon_base
from bossutils.configuration import BossConfig
from spdb.spatialdb import SpatialDB
from spdb.spatialdb.error import SpdbError
from spdb.c_lib import ndlib


class CacheMissDaemon(daemon_base.DaemonBase):

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
                               "s3_index_table": config['aws']['s3-index-table'],
                               "id_index_table": config['aws']['id-index-table'],
                               "id_count_table": config['aws']['id-count-table']}

        sp = SpatialDB(kvio_config, state_config, object_store_config)
        self.set_spatialdb(sp)

    def process(self):
        """Check for a cache miss and add cuboids to the prefetch queue if miss found.
        """
        missed_key = self.get_cache_miss()
        if missed_key is None:
            return

        cache_keys = self.compute_prefetch_keys(missed_key)

        for cache_key in cache_keys:
            if not self.in_s3(cache_key):
                continue

            if self.in_cache(cache_key):
                # The cuboid was paged-in since it showed up in cache-miss.
                continue

            # add cuboid to prefetch queue
            self.enqueue_to_prefetch(cache_key)

    def get_cache_miss(self):
        """Get next cache-cuboid key in the CACHE-MISS list.

        Returns:
            (string|None): None if the list is empty.
        """
        _bytes = self._sp.cache_state.status_client.lpop('CACHE-MISS')
        if _bytes is None:
            return None
        return str(_bytes, 'utf-8')

    def compute_prefetch_keys(self, missed_key):
        """From the missed key, determine what to prefetch.

        Args:
            missed_key (string): Cached-cuboid key.

        Returns:
            (list): List of cache-cuboid keys to fetch.
        """
        key_parts = missed_key.rsplit('&', 1)
        morton_id = key_parts[1]
        coords = ndlib.MortonXYZ(int(morton_id))
        z = coords[2]
        coords_above = coords.copy()
        coords_above[2] = z + 1
        mortonid_above = ndlib.XYZMorton(coords_above)
        key_above = '{}&{}'.format(key_parts[0], mortonid_above)

        if z - 1 < 0:
            return [key_above]

        coords_below = coords.copy()
        coords_below[2] = z - 1
        mortonid_below = ndlib.XYZMorton(coords_below)
        key_below = '{}&{}'.format(key_parts[0], mortonid_below)

        return [key_above, key_below]

    def in_s3(self, cache_key):
        """Determine if the cuboid identified by a cached-cuboid key exists in S3.

        Args:
            cache_key (string): Key identifying cuboid.

        Returns:
            (bool): True if cuboid in S3.
        """
        exists, foo = self._sp.objectio.cuboids_exist([cache_key])
        if len(exists) == 0:
            return False
        return True

    def in_cache(self, cache_key):
        """Check if cuboid is already in the cache.

        Args:
            cache_key (string): This is the cache-cuboid key.

        Returns:
            (bool): True if cuboid in cache.
        """
        try:
            return self._sp.kvio.cube_exists(cache_key)
        except SpdbError:
            # Assume not present if there's an error.
            return False

    def enqueue_to_prefetch(self, cache_key):
        """Add object to prefetch queue using its object-cuboid key.

            The cuboid's cache-cuboid key is converted to an object-cuboid key.

        Args:
            cache_key (string): Key identifying cuboid.
        """
        obj_key = self._sp.objectio.cached_cuboid_to_object_keys([cache_key])
        self._sp.cache_state.status_client.rpush('PRE-FETCH', obj_key[0])

if __name__ == '__main__':
    CacheMissDaemon("boss-cachemissd.pid").main()
