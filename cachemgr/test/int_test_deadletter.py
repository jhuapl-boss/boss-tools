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
import time
import unittest
from unittest.mock import patch
import spdb

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_deadletterd import DeadLetterDaemon

class TestIntegrationDeadLetterDaemon(unittest.TestCase):

    def setUp(self):
        self.dead_letter = DeadLetterDaemon('foo')
        self.dead_letter.configure()

    def test_set_write_locked(self):
        key = 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'
        lookup_key = '1&1&1'
        # Make sure we're starting unlocked.
        self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)

        msg = { "write_cuboid_key": key }
        self.dead_letter.sqs_client.send_message(
            QueueUrl=self.dead_letter.dead_letter_queue,
            MessageBody=json.dumps(msg)
        )

        # Wait for message to enqueue.
        time.sleep(5)

        # Shouldn't have a write lock, yet.
        self.assertFalse(self.dead_letter._sp.cache_state.project_locked(lookup_key))

        try:
            with patch.object(
                self.dead_letter, 'send_alert', wraps=self.dead_letter.send_alert
                ) as send_alert_spy:

                # Method under test.  Returns True if it found a message.
                self.assertTrue(self.dead_letter.check_queue())

                self.assertTrue(self.dead_letter._sp.cache_state.project_locked(lookup_key))

                # Ensure method that publishes to SNS topic called.
                no_proj_info = ''
                send_alert_spy.assert_called_with(lookup_key, no_proj_info)

        finally:
            # Make sure write lock is unset before terminating.
            self.dead_letter._sp.cache_state.set_project_lock(lookup_key, False)
