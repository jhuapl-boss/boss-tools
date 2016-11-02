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

import unittest
import numpy as np

from spdb.spatialdb import Cube, SpatialDB

from cachemgr.boss_delayedwrited import DelayedWriteDaemon
from spdb.spatialdb.test.setup import AWSSetupLayer

import redis
import time


class DelayedWriteDaemonIntegrationTestMixin(object):

    def test_delayed_write_daemon_simple(self):
        """Test handling delayed writes"""
        sp = SpatialDB(self.kvio_config,
                       self.state_config,
                       self.object_store_config)
        dwd = DelayedWriteDaemon("boss-delayedwrited-test.pid")

        # Create some a single delayed write
        cube1 = Cube.create_cube(self.resource, [512, 512, 16])
        cube1.random()
        cube1.morton_id = 0
        res = 0
        time_sample = 0

        write_cuboid_base = "WRITE-CUBOID&{}&{}".format(self.resource.get_lookup_key(), 0)

        write_cuboid_key = sp.kvio.insert_cube_in_write_buffer(write_cuboid_base, res, cube1.morton_id,
                                                               cube1.to_blosc_by_time_index(time_sample))

        sp.cache_state.add_to_delayed_write(write_cuboid_key,
                                            self.resource.get_lookup_key(),
                                            res,
                                            cube1.morton_id,
                                            time_sample,
                                            self.resource.to_json())

        # Use Daemon To handle writes
        dwd.process(sp)
        time.sleep(30)

        # Make sure they went through
        cube2 = sp.cutout(self.resource, (0, 0, 0), (512, 512, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure delay key got deleted
        keys = sp.cache_state.get_all_delayed_write_keys()
        assert not keys

    def test_delayed_write_daemon_multiple(self):
        """Test handling multiple delayed writes"""
        sp = SpatialDB(self.kvio_config,
                       self.state_config,
                       self.object_store_config)
        dwd = DelayedWriteDaemon("boss-delayedwrited-test.pid")

        # Create some a single delayed write
        cube1 = Cube.create_cube(self.resource, [512, 512, 16])
        cube1.random()
        cube1.morton_id = 0
        res = 0
        time_sample = 0
        cube1.data[0, 5, 100, 100] = 2
        cube1.data[0, 5, 100, 101] = 2
        cube1.data[0, 5, 100, 102] = 2

        write_cuboid_base = "WRITE-CUBOID&{}&{}".format(self.resource.get_lookup_key(), 0)

        write_cuboid_key = sp.kvio.insert_cube_in_write_buffer(write_cuboid_base, res, cube1.morton_id,
                                                               cube1.to_blosc_by_time_index(time_sample))

        sp.cache_state.add_to_delayed_write(write_cuboid_key,
                                            self.resource.get_lookup_key(),
                                            res,
                                            cube1.morton_id,
                                            time_sample,
                                            self.resource.to_json())

        cube2 = Cube.create_cube(self.resource, [512, 512, 16])
        cube2.random()
        cube2.morton_id = 0
        res = 0
        time_sample = 0
        cube2.data[0, 5, 100, 100] = 0
        cube2.data[0, 5, 100, 101] = 1
        cube2.data[0, 5, 100, 102] = 0
        cube2.data[0, 5, 100, 103] = 0
        write_cuboid_key = sp.kvio.insert_cube_in_write_buffer(write_cuboid_base, res, cube2.morton_id,
                                                               cube2.to_blosc_by_time_index(time_sample))

        sp.cache_state.add_to_delayed_write(write_cuboid_key,
                                            self.resource.get_lookup_key(),
                                            res,
                                            cube2.morton_id,
                                            time_sample,
                                            self.resource.to_json())

        # Use Daemon To handle writes
        dwd.process(sp)
        time.sleep(30)

        # Make sure they went through
        cube3 = sp.cutout(self.resource, (0, 0, 0), (512, 512, 16), 0)

        cube2.data[0, 5, 100, 100] = 2
        cube2.data[0, 5, 100, 101] = 1
        cube2.data[0, 5, 100, 102] = 2
        cube2.data[0, 5, 100, 103] = cube1.data[0, 5, 100, 103]

        np.testing.assert_array_equal(cube3.data, cube2.data)

        # Make sure delay key got deleted
        keys = sp.cache_state.get_all_delayed_write_keys()
        assert not keys


class TestIntegrationDelayedWriteDaemon(DelayedWriteDaemonIntegrationTestMixin, unittest.TestCase):
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

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
