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
from spdb.spatialdb.rediskvio import RedisKVIO
from cachemgr.boss_sqs_watcherd import SqsWatcher
import redis
import time
from botocore.exceptions import ClientError
import boto3
from moto import mock_lambda

from bossutils import configuration
from bossutils.aws import get_region
from spdb.c_lib import ndlib
import json

#@patch('redis.StrictRedis', mock_strict_redis_client)
class SqsWatcherIntegrationTestMixin(object):

    def test_sqs_watcher_send_message(self):
        """Inject message into queue and test that SqsWatcher kicks off a lambda and writes cuboid to s3."""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 128, 128))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        self.kvio = RedisKVIO(self.kvio_config)

        base_write_cuboid_key = "WRITE-CUBOID&{}&{}".format(self.resource.get_lookup_key(), 0)
        morton_idx = ndlib.XYZMorton([0, 0, 0])
        t = 0
        write_cuboid_key = self.kvio.insert_cube_in_write_buffer(base_write_cuboid_key, t, morton_idx,
                                                                 cube1.to_blosc_by_time_index(t))

        # Put page out job on the queue
        sqs = boto3.client('sqs', region_name=get_region())

        msg_data = {"config": self.config_data,
                    "write_cuboid_key": write_cuboid_key,
                    "lambda-name": "s3_flush",
                    "resource": self.resource.to_dict()}

        response = sqs.send_message(QueueUrl=self.object_store_config["s3_flush_queue"],
                                    MessageBody=json.dumps(msg_data))
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        watcher = SqsWatcher(self.lambda_data)
        #  verify_queue() needs the be run multiple times to verify that the queue is not changing
        #  only then does it send off a lambda message.
        time.sleep(5)
        watcher.verify_queue()
        time.sleep(5)
        lambdas_invoked = watcher.verify_queue()
        if lambdas_invoked < 1:
            time.sleep(5)
            watcher.verify_queue()
        time.sleep(15)
        lambda_client = boto3.client('sqs', region_name=get_region())
        client = boto3.client('sqs', region_name=get_region())
        response = client.get_queue_attributes(
            QueueUrl=self.object_store_config["s3_flush_queue"],
            AttributeNames=[
                'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        https_status_code = response['ResponseMetadata']['HTTPStatusCode']
        queue_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        # test that the queue count is now 0
        assert queue_count == 0

        s3 = boto3.client('s3', region_name=get_region())
        objects_list = s3.list_objects(Bucket=self.object_store_config['cuboid_bucket'])
        # tests that bucket has some Contents.
        assert "Contents" in objects_list.keys()





class TestIntegrationSqsWatcher(SqsWatcherIntegrationTestMixin, unittest.TestCase):

    def tearDown(self):
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
        #super(TestIntegrationSqsWatcher, cls).setUpClass()
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

        # write out data for lambda now that we have updated s3_flush_queue
        cls.config_data = {"kv_config": cls.kvio_config,
                       "state_config": cls.state_config,
                       "object_store_config": cls.object_store_config}

        cls.lambda_data = {"config": cls.config_data,
                            "lambda-name": "s3_flush"}


    @classmethod
    def tearDownClass(cls):
        #super(TestIntegrationSpatialDBImage8Data, cls).tearDownClass()
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

