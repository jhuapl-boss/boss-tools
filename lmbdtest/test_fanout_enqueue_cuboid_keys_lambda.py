# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# lambdafcns contains symbolic links to lambda functions in boss-tools/lambda.
# Since lambda is a reserved word, this allows importing from that folder 
# without updating scripts responsible for deploying the lambda code.
from lambdafcns.fanout_enqueue_cuboid_keys_lambda import (
    build_subargs, SQS_MAX_BATCH)

import json
import unittest
from unittest.mock import patch

class TestFanoutEnqueueCuboidKeysLambda(unittest.TestCase):

    def test_max_batch_is_10(self):
        """
        All of these tests assume the maximum number of messages for a batch
        enqueue is 10.
        """
        self.assertEqual(10, SQS_MAX_BATCH, 'SQS_MAX_BATCH value changed.  Tests will fail!')


    def test_build_subargs_1_key(self):
        key1 = {'object-key': {'S': 'k1'}, 'version-node': {'N': '0'}}
        event = {
            'obj_keys': [key1],
            'config': {}
        }

        actual = build_subargs(event)

        self.assertEqual(1, len(actual))
        self.assertEqual(1, len(actual[0]['cuboid_msgs']))
        self.assertEqual(key1, json.loads(actual[0]['cuboid_msgs'][0]['MessageBody']))
        self.assertEqual(str(0), actual[0]['cuboid_msgs'][0]['Id'])


    def test_build_subargs_less_than_10_keys(self):
        key1 = {'object-key': {'S': 'k1'}, 'version-node': {'N': '0'}}
        key2 = {'object-key': {'S': 'k2'}, 'version-node': {'N': '0'}}
        event = {
            'obj_keys': [key1, key2],
            'config': {}
        }

        actual = build_subargs(event)

        self.assertEqual(1, len(actual))
        self.assertEqual(2, len(actual[0]['cuboid_msgs']))
        self.assertEqual(key1, json.loads(actual[0]['cuboid_msgs'][0]['MessageBody']))
        self.assertEqual(str(0), actual[0]['cuboid_msgs'][0]['Id'])
        self.assertEqual(key2, json.loads(actual[0]['cuboid_msgs'][1]['MessageBody']))
        self.assertEqual(str(1), actual[0]['cuboid_msgs'][1]['Id'])


    def test_build_subargs_10_keys(self):
        keys = []
        for i in range(0, 10):
            keys.append({'object-key': {
                'S': 'k{}'.format(i)}, 
                'version-node': {'N': '0'}
            })

        event = {
            'obj_keys': keys,
            'config': {}
        }

        actual = build_subargs(event)

        self.assertEqual(1, len(actual))
        self.assertEqual(10, len(actual[0]['cuboid_msgs']))
        for i in range(0, 10):
            self.assertEqual(keys[i], json.loads(
                actual[0]['cuboid_msgs'][i]['MessageBody']))
            self.assertEqual(str(i), actual[0]['cuboid_msgs'][i]['Id'])


    def test_build_subargs_more_than_10_keys(self):
        keys = []
        for i in range(0, 14):
            keys.append({'object-key': {
                'S': 'k{}'.format(i)}, 
                'version-node': {'N': '0'}
            })

        event = {
            'obj_keys': keys,
            'config': {}
        }

        actual = build_subargs(event)

        self.assertEqual(2, len(actual))

        self.assertEqual(10, len(actual[0]['cuboid_msgs']))
        for i in range(0, 10):
            self.assertEqual(keys[i], json.loads(
                actual[0]['cuboid_msgs'][i]['MessageBody']))
            self.assertEqual(str(i), actual[0]['cuboid_msgs'][i]['Id'])

        self.assertEqual(4, len(actual[1]['cuboid_msgs']))
        for i in range(0, 4):
            self.assertEqual(keys[i+10], json.loads(
                actual[1]['cuboid_msgs'][i]['MessageBody']))
            self.assertEqual(str(i+10), actual[1]['cuboid_msgs'][i]['Id'])
