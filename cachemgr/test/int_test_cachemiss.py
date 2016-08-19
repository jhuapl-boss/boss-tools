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

import bossutils.configuration as configuration
import json
import spdb
from spdb.c_lib import ndlib
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import SetupTests
import unittest
from unittest.mock import patch

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_cachemissd import CacheMissDaemon

class IntegrationTestCacheMissDaemon(unittest.TestCase):

    def setUp(self):
        self.setUpParams()

        self.cache_miss = CacheMissDaemon('foo')
        self.sp = SpatialDB(
            self.kvio_config, self.state_config, self.object_store_config)
        self.cache_miss.set_spatialdb(self.sp)

    def setUpParams(self):
        self.setup_helper = SetupTests()
        # Don't use mock Amazon resources.
        self.setup_helper.mock = False

        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        # kvio settings, 1 is the test DB.
        self.kvio_config = {"cache_host": self.config['aws']['cache'],
                            "cache_db": 1,
                            "read_timeout": 86400}

        # state settings, 1 is the test DB.
        self.state_config = {"cache_state_host": self.config['aws']['cache-state'], "cache_state_db": 1}

        # object store settings
        _, domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        self.s3_flush_queue_name = "intTest.S3FlushQueue.{}".format(domain).replace('.', '-')
        self.object_store_config = {
            "s3_flush_queue": "",
            "cuboid_bucket": "intTest.{}".format(self.config['aws']['cuboid_bucket']),
            "page_in_lambda_function": self.config['lambda']['page_in_function'],
            "page_out_lambda_function": self.config['lambda']['flush_function'],
            "s3_index_table": "intTest.{}".format(self.config['aws']['s3-index-table'])}

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def test_add_to_prefetch(self):
        """
        flush cache
        request middle cuboid
        check prefetch queue
        """
        cube = Cube.create_cube(self.resource, [128, 128, 16])
        cube.data = np.random.randint(1, 254, (1, 16, 128, 128))
        cube_above = Cube.create_cube(self.resource, [128, 128, 16])
        cube_above.data = np.random.randint(1, 254, (1, 16, 128, 128))
        cube_below = Cube.create_cube(self.resource, [128, 128, 16])
        cube_below.data = np.random.randint(1, 254, (1, 16, 128, 128))

        # Write 3 cuboids that are stacked vertically.
        self.sp.write_cuboid(self.resource, (0, 0, 0), 0, cube_below.data)
        self.sp.write_cuboid(self.resource, (0, 0, 16), 0, cube.data)
        self.sp.write_cuboid(self.resource, (0, 0, 32), 0, cube_above.data)

        cube.morton_id = ndlib.XYZMorton([0, 0, 16])
        cube_below.morton_id = ndlib.XYZMorton([0, 0, 0])
        cube_above.morton_id = ndlib.XYZMorton([0, 0, 32])

        cube_below_cache_key, cube_cache_key, cube_above_cache_key = self.sp.kvio.generate_cached_cuboid_keys(
            self.resource, 0, [0],
            [cube_below.morton_id, cube.morton_id, cube_above.morton_id])

        # Make sure cuboids saved.
        cube_act = self.sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)
        np.testing.assert_array_equal(cube_below, cube_act)
        cube_act = self.sp.cutout(self.resource, (0, 0, 1), (128, 128, 16), 0)
        np.testing.assert_array_equal(cube, cube_act)
        cube_act = self.sp.cutout(self.resource, (0, 0, 2), (128, 128, 16), 0)
        np.testing.assert_array_equal(cube_above, cube_act)

        # Clear cache so we can get a cache miss.
        self.sp.kvio.cache_client.flushdb()

        # Get middle cube again.  This should trigger a cache miss.
        cube_act = self.sp.cutout(self.resource, (0, 0, 1), (128, 128, 16), 0)

        # Confirm there is a cache miss.
        miss_actual = self.sp.kvio.cache_client.lindex('CACHE-MISS', 0)
        self.assertEqual(cube_cache_key, miss_actual)

        self.cache_miss.process()

        # Confirm PRE-FETCH has the cache keys for the cube above and below.
        fetch_actual = self.sp.kvio.get('PRE-FETCH')
        self.assertEqual(
            [cube_above_cache_key, cube_below_cache_key], fetch_actual)
