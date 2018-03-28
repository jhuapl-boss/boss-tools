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
from lambdafcns.write_id_index_lambda import handler, get_class_name

import botocore
import json
#from spdb.spatialdb.object_indices import ObjectIndices
import unittest
from unittest.mock import patch

class TestWriteIdIndexLambda(unittest.TestCase):
    def test_handler_ClientError(self):
        event = {
            'id_index_table': 'idIndex',
            's3_index_table': 's3Index',
            'id_count_table': 'idCount',
            'cuboid_bucket': 'cuboidBucket',
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'id_group': ['1', '2', '3'],
            'version': 0,
            'write_id_index_status': {
                'done': False,
                'delay': 0,
                'retries_left': 2
            }
        }

        context = None
        resp = {}

        with patch('lambdafcns.write_id_index_lambda.ObjectIndices') as fake_obj_ind:
            ex = botocore.exceptions.ClientError(resp, 'UpdateItem')
            ex.errno = 10
            ex.message = 'blah'
            ex.strerror = 'blah'
            fake_obj_ind.return_value.write_id_index.side_effect = ex
            with patch(
                'lambdafcns.write_id_index_lambda.get_region', 
                return_value='us-east-1'
            ):
                # Function under test.
                actual = handler(event, context)

        self.assertFalse(actual['write_id_index_status']['done'])
        self.assertGreater(actual['write_id_index_status']['delay'], 0)
        self.assertEqual(1, actual['write_id_index_status']['retries_left'])
        self.assertIn('result', actual)


    def test_handler_raise_ClientError(self):
        """
        Test that error is raised when retries_left == 0.
        """
        event = {
            'id_index_table': 'idIndex',
            's3_index_table': 's3Index',
            'id_count_table': 'idCount',
            'cuboid_bucket': 'cuboidBucket',
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'id_group': ['1', '2', '3'],
            'version': 0,
            'write_id_index_status': {
                'done': False,
                'delay': 0,
                'retries_left': 0
            }
        }

        context = None
        resp = {}

        with patch('lambdafcns.write_id_index_lambda.ObjectIndices') as fake_obj_ind:
            ex = botocore.exceptions.ClientError(resp, 'UpdateItem')
            ex.errno = 10
            ex.message = 'blah'
            ex.strerror = 'blah'
            fake_obj_ind.return_value.write_id_index.side_effect = ex
            with patch(
                'lambdafcns.write_id_index_lambda.get_region', 
                return_value='us-east-1'
            ):
                with self.assertRaises(botocore.exceptions.ClientError):
                    # Function under test.
                    handler(event, context)


    def test_get_class_name(self):
        resp = {}
        ex = botocore.exceptions.ClientError(resp, 'UpdateItem')
        actual = get_class_name(ex.__class__)
        self.assertEqual('ClientError', actual)


    def test_get_class_name_no_period(self):
        actual = get_class_name('foo')
        self.assertIsNone(actual)
