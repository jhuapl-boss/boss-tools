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

from lambdafcns.copy_cuboid_lambda import copy_cuboid, generate_new_key, validate_translation
import botocore
import boto3
import moto
from spdb.spatialdb.object import AWSObjectStore
import unittest
from unittest.mock import patch

class TestCopyCuboidLambda(unittest.TestCase):
    def test_validate_translation_x(self):
        validate_translation([1024, 0, 0])
        with self.assertRaises(ValueError):
            validate_translation([1025, 0, 0])

    def test_validate_translation_y(self):
        validate_translation([0, 2048, 0])
        with self.assertRaises(ValueError):
            validate_translation([0, 2047, 0])

    def test_validate_translation_z(self):
        validate_translation([0, 0, 48])
        with self.assertRaises(ValueError):
            validate_translation([0, 0, 18])

    def test_generate_new_key(self):
        translate = [-5120, -2560, 256]
        coll_id = 5
        exp_id = 3
        chan_id = 8
        lookup_key = '{}&{}&{}'.format(coll_id, exp_id, chan_id)

        # This morton id is (12, 17, 2).
        obj_key = '02e4578abd294eb42c5c625b6340bd59&4&4&30&0&0&8802'

        actual = generate_new_key(obj_key, translate, lookup_key)

        parts = AWSObjectStore.get_object_key_parts(actual)
        self.assertEqual(coll_id, int(parts.collection_id))
        self.assertEqual(exp_id, int(parts.experiment_id))
        self.assertEqual(chan_id, int(parts.channel_id))

        # This morton id is (2, 12, 18).
        self.assertEqual(17576, int(parts.morton_id))

    @patch('lambdafcns.copy_cuboid_lambda.update_s3_index', autospec=True)
    @patch('lambdafcns.copy_cuboid_lambda.s3_copy', autospec=True)
    def test_copy_cuboid(self, fake_s3_copy, fke_update_s3_index):
        """
        This test is currently mainly just to confirm all the right parameters
        are passed.  Basically, functioning as a "compiler".
        """
        coll_id = 5
        exp_id = 3
        chan_id = 8
        event = {
            's3_key': '02e4578abd294eb42c5c625b6340bd59&4&4&30&0&0&8802',
            'translate': [-5120, 2560, 256],
            'region': 'us-east-1',
            'lookup_key': '{}&{}&{}'.format(coll_id, exp_id, chan_id),
            'ingest_job': 29,
            'version': 0,
            'object_store_config': {
                'id_count_table': 'foo',
                'page_in_lambda_function': 'foo',
                'id_index_table': 'foo',
                'cuboid_bucket': 'foo',
                's3_index_table': 'foo',
                'page_out_lambda_function': 'foo',
                's3_flush_queue': 'foo'
            }
        }

        copy_cuboid(event)
