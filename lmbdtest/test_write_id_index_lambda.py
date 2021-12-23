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
import unittest
from unittest.mock import patch

class TestWriteIdIndexLambda(unittest.TestCase):
    def setUp(self):
        self.config = {
            'object_store_config': {
                'id_index_table': 'id-index-ddb',
                's3_index_table': 's3-index-ddb',
                'id_count_table': 'id-count-ddb',
                'cuboid_bucket': 'cuboid-bucket',
                'id_index_new_chunk_threshold': 100,
            },
        }

    def test_write_all_success(self):
        event = {
            'config': self.config,
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'ids': ['1', '2', '3'],
            'version': 0,
            'write_id_index_status': {
                'done': False,
                'delay': 0,
                'retries_left': 2
            }
        }

        context = None

        with patch('lambdafcns.write_id_index_lambda.ObjectIndices'):
            with patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}):
                # Function under test.
                actual = handler(event, context)

        # When all ids successfully updated with the cuboid key, ids
        # should be empty.
        self.assertEqual(0, len(actual['ids']))
        self.assertTrue(actual['write_id_index_status']['done'])

    def test_write_has_a_failure(self):
        event = {
            'config': self.config,
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'ids': ['1', '2', '3', '4', '5'],
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
            # Make the write fail when updating id 3.
            fake_obj_ind.return_value.write_id_index.side_effect = [None, None, ex]
            with patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}):
                # Function under test.
                actual = handler(event, context)

        # Test set up to fail for id 3 above.
        self.assertEqual(['3', '4', '5'], actual['ids'])
        self.assertFalse(actual['write_id_index_status']['done'])

    def test_handler_ClientError(self):
        event = {
            'config': self.config,
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'ids': ['1', '2', '3'],
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
            with patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}):
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
            'config': self.config,
            'id_index_new_chunk_threshold': 100,
            'cuboid_object_key': 'blah',
            'ids': ['1', '2', '3'],
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
            with patch.dict('os.environ', {'AWS_REGION': 'us-east-1'}):
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
