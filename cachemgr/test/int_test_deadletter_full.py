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


import base64
from bossutils.aws import get_region
import bossutils.configuration as configuration
import boto3
from botocore.exceptions import ClientError
import json
import numpy as np
import time
import unittest
from unittest.mock import patch, ANY
import spdb
from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.error import SpdbError, ErrorCodes
from spdb.spatialdb.test.setup import SetupTests
import tempfile
import warnings
from zipfile import ZipFile, ZipInfo

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_deadletterd import DeadLetterDaemon

"""
Perform end-to-end test of the dead letter daemon.  To simulate getting 
messages in the dead letter queue, it replaces the s3 flush lambda with an 
empty lambda function, so messages never leave the flush queue.

To run faster, the test points the dead letter daemon at the flush queue
INSTEAD of the dead letter queue.  Thus it tests that the message format is
as expected, but it does not ensure that the flush queue is configured to
move messages to the dead letter queue after x attempts.
"""
class TestEnd2EndIntegrationDeadLetterDaemon(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """

        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        cls.setUpParams(cls)

        lambda_client = boto3.client('lambda', region_name=get_region())
        cls.test_lambda = 'IntTest-{}'.format(cls.domain).replace('.', '-')
        lambda_client.delete_function(FunctionName=cls.test_lambda)   
        resp = lambda_client.get_function(FunctionName=cls.object_store_config['page_out_lambda_function'])
        lambda_cfg = resp['Configuration']
        vpc_cfg = lambda_cfg['VpcConfig']
        # VpcId is not a valid field when creating a lambda fcn.
        del vpc_cfg['VpcId']

        temp_file = tempfile.NamedTemporaryFile()
        temp_name = temp_file.name + '.zip'
        temp_file.close();
        with ZipFile(temp_name, mode='w') as zip:
            t = time.localtime()
            lambda_file = ZipInfo('lambda_function.py', date_time=(
                t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec))
            # Set file permissions.
            lambda_file.external_attr = 0o777 << 16
            code = 'def handler(event, context):\n    return\n'
            zip.writestr(lambda_file, code)

        with open(temp_name, 'rb') as zip2:
            lambda_bytes = zip2.read()

        lambda_client.create_function(
            FunctionName=cls.test_lambda,
            VpcConfig=vpc_cfg,
            Role=lambda_cfg['Role'],
            Runtime=lambda_cfg['Runtime'],
            Handler='lambda_function.handler',
            MemorySize=128,
            Code={'ZipFile': lambda_bytes }
         )

        # Set page out function to the test lambda.
        cls.object_store_config['page_out_lambda_function'] = cls.test_lambda

        print('standby for queue creation (slow ~30s)')
        try:
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)
        except ClientError:
            try:
                cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
            except:
                pass
            time.sleep(61)
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)

        print('done')

    @classmethod
    def tearDownClass(cls):
        try:
            cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
        except:
            pass

        lambda_client = boto3.client('lambda', region_name=get_region())
        #lambda_client.delete_function(FunctionName=cls.test_lambda)

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
        _, self.domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        self.s3_flush_queue_name = "intTest.S3FlushQueue.{}".format(self.domain).replace('.', '-')
        self.object_store_config = {
            "s3_flush_queue": '', # This will get updated after the queue is created.
            "cuboid_bucket": "intTest.{}".format(self.config['aws']['cuboid_bucket']),
            "page_in_lambda_function": self.config['lambda']['page_in_function'],
            "page_out_lambda_function": self.config['lambda']['flush_function'],
            "s3_index_table": "intTest.{}".format(self.config['aws']['s3-index-table'])}

    def setUp(self):
        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        self.dead_letter = DeadLetterDaemon('foo')
        self.dead_letter.set_spatialdb(SpatialDB(
            self.kvio_config, self.state_config, self.object_store_config))
        self.setup_helper = SetupTests()
        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        # Make the daemon look at the flush queue so we don't need to create
        # a deadletter queue for testing.
        self.dead_letter.dead_letter_queue = (
            self.object_store_config['s3_flush_queue'])


    def test_set_write_locked(self):
        # Cuboid dimensions.
        xy_dim = 128
        z_dim = 16

        lookup_key = self.data['lookup_key']
        # Make sure this key isn't currently locked.
        self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)
        self.assertFalse(self.dead_letter._sp.cache_state.project_locked(lookup_key))

        cube1 = Cube.create_cube(self.resource, [xy_dim, xy_dim, z_dim])
        cube1.data = np.random.randint(1, 254, (1, z_dim, xy_dim, xy_dim))

        # Ensure that the we are not in a page-out state by wiping the entire
        # cache state.
        self.dead_letter._sp.cache_state.status_client.flushdb()
        self.dead_letter._sp.write_cuboid(
            self.resource, (0, 0, 0), 0, cube1.data)


        try:
            with patch.object(
                self.dead_letter, 'send_alert', wraps=self.dead_letter.send_alert
                ) as send_alert_spy:

                # Method under test.  Returns True if it found a message.
                i = 0
                while not self.dead_letter.check_queue() and i < 30:
                    time.sleep(1)
                    i += 1

                self.assertTrue(self.dead_letter._sp.cache_state.project_locked(lookup_key))

                # Ensure method that publishes to SNS topic called.
                send_alert_spy.assert_called_with(lookup_key, ANY)

        finally:
            # Make sure write lock is unset before terminating.
            self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)

