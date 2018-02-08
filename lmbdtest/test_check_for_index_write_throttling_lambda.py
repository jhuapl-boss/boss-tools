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

from lambdafcns.check_for_index_write_throttling_lambda import parse_response

import unittest

class TestCheckForIndexWriteThrottlingLambda(unittest.TestCase):
    def test_no_datapoints_key(self):
        event = {}
        resp = {}

        actual = parse_response(event, resp)

        self.assertIn('write_throttled', actual)
        self.assertFalse(actual['write_throttled'])

    def test_empty_datapoints_array(self):
        event = {}
        resp = {'Datapoints': []}

        actual = parse_response(event, resp)

        self.assertIn('write_throttled', actual)
        self.assertFalse(actual['write_throttled'])

    def test_nonempty_datapoints_array(self):
        event = {}
        resp = {'Datapoints': [{'Sum': 1.0}, {'Sum': 2.0}]}

        actual = parse_response(event, resp)

        self.assertIn('write_throttled', actual)
        self.assertTrue(actual['write_throttled'])
