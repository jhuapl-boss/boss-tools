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
from lambdafcns.fanout_write_id_index_lambda import (
    pack_ids_for_lambdas, IDS_PER_LAMBDA)

import json
import unittest

class TestFanoutWriteIdIndexLambda(unittest.TestCase):

    def test_ids_per_lambda_is_10(self):
        """
        All of these tests assume the maximum number of ids for a lambda is 10.
        """
        self.assertEqual(10, IDS_PER_LAMBDA, 'IDS_PER_LAMBDA value changed.  Tests will likely fail!')


    def test_single_batch_of_ids(self):
        ids = ['23', '10', '55']

        exp  = [{'id_group': ids}]
        actual = pack_ids_for_lambdas(ids)
        self.assertEqual(exp, actual)


    def test_batch_matches_ids_per_lambda(self):
        ids = []
        for i in range(0, IDS_PER_LAMBDA):
            ids.append(str(i))

        exp = [{'id_group': ids}]
        actual = pack_ids_for_lambdas(ids)
        self.assertEqual(exp, actual)


    def test_batch_more_than_ids_per_lambda(self):
        ids = []
        for i in range(0, IDS_PER_LAMBDA):
            ids.append(str(i))

        more_ids = [str(IDS_PER_LAMBDA+1), str(IDS_PER_LAMBDA+2)]
        all_ids = ids + more_ids

        exp = [{'id_group': ids}, {'id_group': more_ids}]

        actual = pack_ids_for_lambdas(all_ids)
        self.assertEqual(exp, actual)
