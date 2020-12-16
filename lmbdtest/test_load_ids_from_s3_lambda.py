# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

import unittest
from unittest.mock import patch
from lambdafcns.load_ids_from_s3_lambda import (
    handler,
    get_chunk_indices,
    IDSET_ATTR,
)


class TestGetChunkIndicesLambda(unittest.TestCase):
    def test_simple_case(self):
        total_num_ids = 800
        chunk_size = 200
        start = 200

        slice0 = get_chunk_indices(start, chunk_size, total_num_ids, 0)
        slice1 = get_chunk_indices(start, chunk_size, total_num_ids, 1)

        self.assertEqual(200, slice0.start)
        self.assertEqual(400, slice0.stop)

        self.assertEqual(400, slice1.start)
        self.assertEqual(600, slice1.stop)

    def test_chunk_exceeds_max_ids_case(self):
        total_num_ids = 500
        chunk_size = 200
        start = 200

        slice0 = get_chunk_indices(start, chunk_size, total_num_ids, 0)
        slice1 = get_chunk_indices(start, chunk_size, total_num_ids, 1)

        self.assertEqual(200, slice0.start)
        self.assertEqual(400, slice0.stop)

        self.assertEqual(400, slice1.start)
        self.assertEqual(500, slice1.stop)


class TestLoadIdsFromS3Lambda(unittest.TestCase):
    def test_without_dynamo(self):
        event = {
            "num_ids_per_worker": 20,
            "id_chunk_size": 5,
            "cuboid_object_key": "fake_cuboid_key",
            "config": {
                "object_store_config": {
                    "s3_index_table": "s3_test_table",
                }
            },
            "version": 0,
            "worker_id": 1,
        }

        exp = dict(event)
        exp["finished"] = False
        exp["id_chunks"] = [
            [20, 21, 22, 23, 24],
            [25, 26, 27, 28, 29],
            [30, 31, 32, 33, 34],
            [35, 36, 37, 38, 39],
        ]

        with patch(
            "lambdafcns.load_ids_from_s3_lambda.read_dynamo", autospec=True
        ) as fake_read:
            fake_read.return_value = {
                "Item": {IDSET_ATTR: {"NS": [i for i in range(45)]}},
            }

            actual = handler(event, None)

            self.assertDictEqual(exp, actual)

    def test_without_dynamo_finished(self):
        event = {
            "num_ids_per_worker": 20,
            "id_chunk_size": 5,
            "cuboid_object_key": "fake_cuboid_key",
            "config": {
                "object_store_config": {
                    "s3_index_table": "s3_test_table",
                }
            },
            "version": 0,
            "worker_id": 1,
        }

        exp = dict(event)
        exp["finished"] = True
        exp["id_chunks"] = []

        with patch(
            "lambdafcns.load_ids_from_s3_lambda.read_dynamo", autospec=True
        ) as fake_read:
            fake_read.return_value = {}
            actual = handler(event, None)
            self.assertDictEqual(exp, actual)
