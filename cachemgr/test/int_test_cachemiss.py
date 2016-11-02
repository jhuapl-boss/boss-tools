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

import numpy as np
import redis

from spdb.spatialdb.test.setup import AWSSetupLayer

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.spatialdb import Cube, SpatialDB
import unittest

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_cachemissd import CacheMissDaemon

"""
Test that a cache miss is properly serviced by the code of the cache miss
daemon.  After popping a miss from the Redis CACHE-MISS, it should add the
cuboids above and below the missed cuboid to Redis PRE-FETCH.
"""


class IntegrationTestCacheMissDaemon(unittest.TestCase):
    layer = AWSSetupLayer

    def setUp(self):

        # Get data from nose2 layer based setup
        self.data = self.layer.data
        self.resource = self.layer.resource
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config

        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

        self.cache_miss = CacheMissDaemon('foo')
        self.sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        self.cache_miss.set_spatialdb(self.sp)

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

        cube = Cube.create_cube(self.resource, [x_dim, y_dim, z_dim])
        cube.random()
        cube_above = Cube.create_cube(self.resource, [x_dim, y_dim, z_dim])
        cube_above.random()
        cube_below = Cube.create_cube(self.resource, [x_dim, y_dim, z_dim])
        cube_below.random()

        # Write 3 cuboids that are stacked vertically.
        self.sp.write_cuboid(self.resource, (0, 0, 0), 0, cube_below.data)
        self.sp.write_cuboid(self.resource, (0, 0, z_dim), 0, cube.data)
        self.sp.write_cuboid(self.resource, (0, 0, z_dim * 2), 0, cube_above.data)

        cube.morton_id = ndlib.XYZMorton([0, 0, z_dim // z_dim])
        cube_below.morton_id = ndlib.XYZMorton([0, 0, 0])
        cube_above.morton_id = ndlib.XYZMorton([0, 0, z_dim * 2 // z_dim])
        print('mortons: {}, {}, {}'.format(
            cube_below.morton_id, cube.morton_id, cube_above.morton_id))

        cube_below_cache_key, cube_cache_key, cube_above_cache_key = self.sp.kvio.generate_cached_cuboid_keys(
            self.resource, 0, [0],
            [cube_below.morton_id, cube.morton_id, cube_above.morton_id])

        # Make sure cuboids saved.
        cube_act = self.sp.cutout(self.resource, (0, 0, 0), (x_dim, y_dim, z_dim), 0)
        np.testing.assert_array_equal(cube_below.data, cube_act.data)
        cube_act = self.sp.cutout(self.resource, (0, 0, z_dim), (x_dim, y_dim, z_dim), 0)
        np.testing.assert_array_equal(cube.data, cube_act.data)
        cube_act = self.sp.cutout(self.resource, (0, 0, z_dim * 2), (x_dim, y_dim, z_dim), 0)
        np.testing.assert_array_equal(cube_above.data, cube_act.data)

        # Clear cache so we can get a cache miss.
        self.sp.kvio.cache_client.flushdb()

        # Also clear CACHE-MISS before running testing.
        self.sp.cache_state.status_client.flushdb()

        # Get middle cube again.  This should trigger a cache miss.
        cube_act = self.sp.cutout(self.resource, (0, 0, z_dim), (x_dim, y_dim, z_dim), 0)

        # Confirm there is a cache miss.
        misses = self.sp.cache_state.status_client.lrange('CACHE-MISS', 0, 10)
        print('misses:')
        print(misses)
        miss_actual = self.sp.cache_state.status_client.lindex('CACHE-MISS', 0)
        self.assertEqual(cube_cache_key, str(miss_actual, 'utf-8'))

        # This is the system under test.
        self.cache_miss.process()

        # Confirm PRE-FETCH has the object keys for the cube above and below.
        fetch_actual1 = self.sp.cache_state.status_client.lindex('PRE-FETCH', 0)
        fetch_actual2 = self.sp.cache_state.status_client.lindex('PRE-FETCH', 1)
        obj_keys = self.sp.objectio.cached_cuboid_to_object_keys(
            [cube_above_cache_key, cube_below_cache_key])
        self.assertEqual(obj_keys[0], str(fetch_actual1, 'utf-8'))
        self.assertEqual(obj_keys[1], str(fetch_actual2, 'utf-8'))
