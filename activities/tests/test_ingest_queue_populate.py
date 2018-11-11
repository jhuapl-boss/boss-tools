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

import boto3
import ingest_queue_populate as iqp
import unittest
from unittest.mock import patch, MagicMock

@patch('boto3.resource')
class TestIngestQueueUploadLambda(unittest.TestCase):

    def test_get_tile_count(self, fake_resource):
        args = {
            "upload_sfn": "IngestUpload",
            "x_start": 0,
            "x_stop": 2048,
            "y_start": 0,
            "y_stop": 2048,
            "z_start": 0,
            "z_stop": 20,
            "t_start": 0,
            "t_stop": 1,
            "project_info": [
              "3",
              "3",
              "3"
            ],
            "ingest_queue": "https://queue.amazonaws.com/...",
            "job_id": 11,
            "t_tile_size": 1,
            "upload_queue": "https://queue.amazonaws.com/...",
            "x_tile_size": 1024,
            "y_tile_size": 1024,
            "z_tile_size": 1,
            "resolution": 0,
            "tiles_to_skip": 1000,
            'MAX_NUM_TILES_PER_LAMBDA': 30000,
            'z_chunk_size': 16

        }

        actual = iqp.get_tile_count(args)
        self.assertEqual(80, actual)

