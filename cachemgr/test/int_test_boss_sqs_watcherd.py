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
from spdb.spatialdb.rediskvio import RedisKVIO
from cachemgr.boss_sqs_watcherd import SqsWatcher
import redis
import time
import boto3
from spdb.spatialdb.test.setup import AWSSetupLayer
from spdb.project import BossResourceBasic

from bossutils.aws import get_region
from spdb.c_lib import ndlib
import json


class SqsWatcherIntegrationTestMixin(object):

    def test_sqs_watcher_send_message(self):
        """Inject message into queue and test that SqsWatcher kicks off a lambda and writes cuboid to s3."""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [512, 512, 16])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        base_write_cuboid_key = "WRITE-CUBOID&{}&{}".format(self.resource.get_lookup_key(), 0)
        morton_idx = ndlib.XYZMorton([0, 0, 0])
        t = 0
        write_cuboid_key = sp.kvio.insert_cube_in_write_buffer(base_write_cuboid_key, t, morton_idx,
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
    layer = AWSSetupLayer

    def setUp(self):
        # Get data from nose2 layer based setup

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

        self.config_data = {"kv_config": self.kvio_config,
                            "state_config": self.state_config,
                            "object_store_config": self.object_store_config}

        self.lambda_data = {"config": self.config_data,
                            "lambda-name": "s3_flush"}

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
