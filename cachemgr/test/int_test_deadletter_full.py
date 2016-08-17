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
import numpy as np
import time
import unittest
from unittest.mock import patch, ANY
import spdb
from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.error import SpdbError, ErrorCodes
from spdb.spatialdb.test.setup import SetupTests
import boto3

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_deadletterd import DeadLetterDaemon

"""
Perform end-to-end test.  Requires the s3 flush lambda to be modified to
fail.  May need to poll the SQS s3 flush queue via the AWS console to get
enough failed deliveries to move the message to the dead letter queue.
"""
class TestEnd2EndIntegrationDeadLetterDaemon(unittest.TestCase):

    def setUp(self):
        self.dead_letter = DeadLetterDaemon('foo')
        self.dead_letter.configure()
        self.setup_helper = SetupTests()
        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)


    def test_set_write_locked(self):
        lookup_key = self.data['lookup_key']
        # Make sure this key isn't currently locked.
        self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)
        self.assertFalse(self.dead_letter._sp.cache_state.project_locked(lookup_key))

        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 128, 128))

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
                while not self.dead_letter.check_queue():
                    time.sleep(1)

                self.assertTrue(self.dead_letter._sp.cache_state.project_locked(lookup_key))

                # Ensure method that publishes to SNS topic called.
                send_alert_spy.assert_called_with(lookup_key, ANY)

        finally:
            # Make sure write lock is unset before terminating.
            self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)

