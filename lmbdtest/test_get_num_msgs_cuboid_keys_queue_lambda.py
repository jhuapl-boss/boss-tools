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

from lambdafcns.get_num_msgs_cuboid_keys_queue_lambda import (
    parse_num_msgs_response, BadSqsResponse
)

import unittest

class TestDequeueCuboidKeysLambda(unittest.TestCase):

    def test_no_attributes_in_reponse(self):
        event = {}
        resp = {}
        with self.assertRaises(BadSqsResponse):
            parse_num_msgs_response(event, resp)

    def test_no_approx_num_msgs_in_response(self):
        event = {}
        resp = {'Attributes': {}}
        with self.assertRaises(BadSqsResponse):
            parse_num_msgs_response(event, resp)
