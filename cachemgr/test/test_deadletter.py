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
from unittest.mock import patch
import spdb
import bossutils.configuration as configuration

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_deadletterd import DeadLetterDaemon

class TestDeadLetterDaemon(unittest.TestCase):

    def setUp(self):
        self.dead_letter = DeadLetterDaemon('foo')

    def test_zero_msgs(self):
        """Make sure empty array doesn't cause an error."""
        msg = []
        self.dead_letter.handle_messages(msg)

    def test_message_missing_body(self):
        """Make sure message without 'Body' key doesn't cause an error."""
        msg = [{}]
        self.dead_letter.handle_messages(msg)

    def test_extract_lookup_key(self):
        key = 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'
        exp = '1&1&1'
        actual = self.dead_letter.extract_lookup_key(key)
        self.assertEqual(exp, actual)

    @patch('spdb.spatialdb.CacheStateDB', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_already_write_locked(self, sp, state):
        key = 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'
        lookup_key = '1&1&1'
        msg = [{"Body": {
                "write_cuboid_key": key
            }
        }]

        # Use mock spatialdb.
        self.dead_letter.set_spatialdb(sp)
        sp.cache_state = state

        # Make this key appear to be already write locked.
        state.project_locked.return_value = True

        # Method under test.
        self.dead_letter.handle_messages(msg)

        # Ensure write lock checked with proper key.
        state.project_locked.assert_called_with(lookup_key)

        # Lock status should not be altered if already write locked.
        state.set_project_lock.assert_not_called()

    # @patch.object('boss_deadletterd.DeadLetterDaemon', 'send_alert', autospec=True)
    @patch('spdb.spatialdb.CacheStateDB', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_set_write_locked(self, sp, state):
        key = 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'
        lookup_key = '1&1&1'
        msg = [{"Body": {
                "write_cuboid_key": key
            }
        }]

        # Use mock spatialdb.
        self.dead_letter.set_spatialdb(sp)
        sp.cache_state = state

        # Make this key appear to be not write locked.
        state.project_locked.return_value = False

        with patch.object(self.dead_letter, 'send_alert') as send_alert_fake:
            # Method under test.
            self.dead_letter.handle_messages(msg)

            # Ensure write lock checked with proper key.
            state.project_locked.assert_called_with(lookup_key)

            # Write lock should be set.
            state.set_project_lock.assert_called_with(lookup_key, True)

            # Alert should be sent.
            send_alert_fake.assert_called_with(lookup_key)
