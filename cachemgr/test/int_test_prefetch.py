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
import spdb
from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import SetupTests
import time
import unittest
from unittest.mock import patch
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
    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """

        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        cls.setUpParams(cls)

        try:
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])
        except ClientError:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])

        try:
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except ClientError:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])

        try:
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)
        except ClientError:
            try:
                cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
            except:
                pass
            time.sleep(61)
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
        except:
            pass

        try:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except:
            pass

        try:
            cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
        except:
            pass


    def setUp(self):
        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        self.prefetch = PrefetchDaemon('foo')
        self.sp = SpatialDB(
            self.kvio_config, self.state_config, self.object_store_config)
        self.prefetch.set_spatialdb(self.sp)

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
        self.state_config = {
            "cache_state_host": self.config['aws']['cache-state'],
            "cache_state_db": 1}

        # object store settings
        _, domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        self.s3_flush_queue_name = "intTest.S3FlushQueue.{}".format(domain).replace('.', '-')
        self.object_store_config = {
            "s3_flush_queue": '', # This will get updated after the queue is created.
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
        cuboid_dims = CUBOIDSIZE[0]
        # Cuboid dimensions.
        x_dim = cuboid_dims[0]
        y_dim = cuboid_dims[1]
        z_dim = cuboid_dims[2]

        cube_above = Cube.create_cube(self.resource, [x_dim, y_dim, z_dim])
        cube_above.data = np.random.randint(1, 254, (1, z_dim, y_dim, x_dim))

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

