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
import spdb
from spdb.c_lib import ndlib
import unittest
from unittest.mock import patch

# Add a reference to parent so that we can import those files.
import os
import sys
cur_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.normpath(os.path.join(cur_dir, '..'))
sys.path.append(parent_dir)
from boss_cachemissd import CacheMissDaemon

class TestCacheMissDaemon(unittest.TestCase):

    def setUp(self):
        self.cache_miss = CacheMissDaemon('foo')

    def test_compute_prefetch_keys(self):
        xyz = [5, 10, 14]
        mortonid = ndlib.XYZMorton(xyz)
        mortonid_above = ndlib.XYZMorton([5, 10, 15])
        mortonid_below = ndlib.XYZMorton([5, 10, 13])
        key_prefix = 'CACHED-CUBOID&1&2&3&4&5&'
        missed_key = '{}{}'.format(key_prefix, mortonid)
        expected = [
            '{}{}'.format(key_prefix, mortonid_above),
            '{}{}'.format(key_prefix, mortonid_below)
        ]
        actual = self.cache_miss.compute_prefetch_keys(missed_key)

        self.assertEqual(expected, actual)

    def test_compute_prefetch_keys_at_z0(self):
        xyz = [5, 10, 0]
        mortonid = ndlib.XYZMorton(xyz)
        mortonid_above = ndlib.XYZMorton([5, 10, 1])
        key_prefix = 'CACHED-CUBOID&1&2&3&4&5&'
        missed_key = '{}{}'.format(key_prefix, mortonid)
        expected = [
            '{}{}'.format(key_prefix, mortonid_above),
        ]
        actual = self.cache_miss.compute_prefetch_keys(missed_key)

        self.assertEqual(expected, actual)

    @patch('spdb.spatialdb.AWSObjectStore', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_in_s3_false(self, sp, objectio):
        # Use mock spatialdb.
        self.cache_miss.set_spatialdb(sp)
        sp.objectio = objectio
        objectio.cuboids_exist.return_value = ([], [0])

        key = 'CACHED-CUBOID&1&2&3&4&5&132'
        self.assertFalse(self.cache_miss.in_s3(key))

    @patch('spdb.spatialdb.AWSObjectStore', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_in_s3_true(self, sp, objectio):
        # Use mock spatialdb.
        self.cache_miss.set_spatialdb(sp)
        sp.objectio = objectio
        objectio.cuboids_exist.return_value = ([0], [])

        key = 'CACHED-CUBOID&1&2&3&4&5&132'
        self.assertTrue(self.cache_miss.in_s3(key))

    @patch('spdb.spatialdb.RedisKVIO', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_in_cache_false(self, sp, kvio):
        # Use mock spatialdb.
        self.cache_miss.set_spatialdb(sp)
        sp.kvio = kvio
        kvio.cube_exists.return_value = False

        key = 'CACHED-CUBOID&1&2&3&4&5&132'
        self.assertFalse(self.cache_miss.in_cache(key))

    @patch('spdb.spatialdb.RedisKVIO', autospec=True)
    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_in_cache_true(self, sp, kvio):
        # Use mock spatialdb.
        self.cache_miss.set_spatialdb(sp)
        sp.kvio = kvio
        kvio.cube_exists.return_value = True

        key = 'CACHED-CUBOID&1&2&3&4&5&132'
        self.assertTrue(self.cache_miss.in_cache(key))
