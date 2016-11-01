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

from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import SetupTests

from cachemgr.boss_delayedwrited import DelayedWriteDaemon

import redis
import time
from botocore.exceptions import ClientError

from bossutils import configuration


class DelayedWriteDaemonIntegrationTestMixin(object):

    def test_delayed_write_daemon_simple(self):
        """Test handling delayed writes"""
        sp = SpatialDB(self.kvio_config,
                       self.state_config,
                       self.object_store_config)
        dwd = DelayedWriteDaemon("boss-delayedwrited-test.pid")

        # Create some a single delayed write
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 128, 128))
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
        cube2 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

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
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(4, 254, (1, 16, 128, 128))
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

        cube2 = Cube.create_cube(self.resource, [128, 128, 16])
        cube2.data = np.random.randint(4, 254, (1, 16, 128, 128))
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
        cube3 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

        cube2.data[0, 5, 100, 100] = 2
        cube2.data[0, 5, 100, 101] = 1
        cube2.data[0, 5, 100, 102] = 2
        cube2.data[0, 5, 100, 103] = cube1.data[0, 5, 100, 103]

        np.testing.assert_array_equal(cube3.data, cube2.data)

        # Make sure delay key got deleted
        keys = sp.cache_state.get_all_delayed_write_keys()
        assert not keys


class TestIntegrationDelayedWriteDaemon(DelayedWriteDaemonIntegrationTestMixin, unittest.TestCase):

    def setUp(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def setUpParams(self):
        """ Create a diction of configuration values for the test resource. """
        # setup resources
        self.setup_helper = SetupTests()
        self.setup_helper.mock = False

        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        # kvio settings
        self.kvio_config = {"cache_host": self.config['aws']['cache'],
                            "cache_db": 1,
                            "read_timeout": 86400}

        # state settings
        self.state_config = {"cache_state_host": self.config['aws']['cache-state'], "cache_state_db": 1}

        # object store settings
        _, domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        self.s3_flush_queue_name = "intTest.S3FlushQueue.{}".format(domain).replace('.', '-')
        self.object_store_config = {"s3_flush_queue": "",
                                    "cuboid_bucket": "intTest.{}".format(self.config['aws']['cuboid_bucket']),
                                    "page_in_lambda_function": self.config['lambda']['page_in_function'],
                                    "page_out_lambda_function": self.config['lambda']['flush_function'],
                                    "s3_index_table": "intTest.{}".format(self.config['aws']['s3-index-table'])}

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """
        super(TestIntegrationDelayedWriteDaemon, cls).setUpClass()
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
        super(TestIntegrationDelayedWriteDaemon, cls).tearDownClass()
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

    def get_num_cache_keys(self, spdb):
        return len(self.cache_client.keys("*"))

    def get_num_status_keys(self, spdb):
        return len(self.status_client.keys("*"))
