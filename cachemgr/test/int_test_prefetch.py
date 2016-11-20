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
from botocore.exceptions import ClientError
import numpy as np
import redis
from spdb.spatialdb.test.setup import AWSSetupLayer
from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import SetupTests
import time
import unittest
import warnings

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_prefetchd import PrefetchDaemon

"""
Test prefetch daemon using a real redis instance and actual cuboid.
"""


class IntegrationTestPrefetchDaemon(unittest.TestCase):
    layer = AWSSetupLayer

    def setUp(self):

        # Setup Data
        self.data = self.layer.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        # Setup config
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config

        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')
        self.prefetch = PrefetchDaemon('foo')
        self.sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        self.prefetch.set_spatialdb(self.sp)

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def test_add_to_prefetch(self):
        cuboid_dims = CUBOIDSIZE[0]
        # Cuboid dimensions.
        x_dim = cuboid_dims[0]
        y_dim = cuboid_dims[1]
        z_dim = cuboid_dims[2]

        cube_above = Cube.create_cube(self.resource, [x_dim, y_dim, z_dim])
        cube_above.random()

        # Write cuboid that are stacked vertically.
        self.sp.write_cuboid(self.resource, (0, 0, z_dim * 2), 0, cube_above.data)

        cube_above.morton_id = ndlib.XYZMorton([0, 0, z_dim * 2 // z_dim])

        cube_above_cache_key = self.sp.kvio.generate_cached_cuboid_keys(
            self.resource, 0, [0], [cube_above.morton_id])

        # Make sure cuboid saved.
        cube_act = self.sp.cutout(self.resource, (0, 0, z_dim * 2), (x_dim, y_dim, z_dim), 0)
        np.testing.assert_array_equal(cube_above.data, cube_act.data)

        # Clear cache so we can test prefetch.
        self.sp.kvio.cache_client.flushdb()

        # Also clear cache state before running test.
        self.sp.cache_state.status_client.flushdb()

        obj_keys = self.sp.objectio.cached_cuboid_to_object_keys(
            cube_above_cache_key)

        # Place a cuboid in the pretch queue.
        self.sp.cache_state.status_client.rpush('PRE-FETCH', obj_keys[0])

        # This is the system under test.
        self.prefetch.process()

        # Wait for cube to be prefetched.
        i = 0
        while not self.sp.kvio.cube_exists(cube_above_cache_key[0]) and i < 30:
            time.sleep(1)
            i += 1

        # Confirm cuboid now in cache.
        self.assertTrue(self.sp.kvio.cube_exists(cube_above_cache_key[0]))

        cube_act = self.sp.cutout(
            self.resource, (0, 0, z_dim * 2), (x_dim, y_dim, z_dim), 0)
        np.testing.assert_array_equal(cube_above.data, cube_act.data)

