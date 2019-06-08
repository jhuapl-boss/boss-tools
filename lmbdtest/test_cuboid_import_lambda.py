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

from bossnames.names import AWSNames
from bossnames.bucket_object_tags import TAG_DELETE_KEY, TAG_DELETE_VALUE
import boto3
import io
import json
from lambdafcns.cuboid_import_lambda import run, get_object_store_cfg
from moto import mock_s3
import unittest
from unittest.mock import patch
import tempfile

class Context:
    """
    Simulate the lambda Context object.
    """
    def __init__(self):
        self.function_name = 'CuboidImportLambda.theboss.io'
BUCKET_NAME = 'ingest.theboss.io'
REGION = 'us-east-1'

class TestCuboidImportLambda(unittest.TestCase):

    """
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

    def tearDown(self):
        self.mock_s3.stop()
    """

    @patch('lambdafcns.cuboid_import_lambda.update_s3_index', autospec=True)
    @patch('lambdafcns.cuboid_import_lambda.s3_copy', autospec=True)
    def test_run_bad_bucket(self, fake_s3_copy, fake_update_s3_index):
        """
        Test that an exception occurs if the trigger bucket is not correct.
        """
        event = {
            'Records': [
                {
                    's3': {
                        'object': { 'key': '02e4578abd294eb42c5c625b6340bd59&4&4&30&0&0&8802' },
                        'bucket': { 'name': 'some_bucket' } } 
                }],
            'region': REGION   # This is added by the lambda handler function.
        }

        with self.assertRaises(ValueError):
            run(event, Context())

    @patch('lambdafcns.cuboid_import_lambda.s3_mark_for_deletion', autospec=True)
    @patch('lambdafcns.cuboid_import_lambda.get_object_metadata', autospec=True)
    @patch('lambdafcns.cuboid_import_lambda.update_s3_index', autospec=True)
    @patch('lambdafcns.cuboid_import_lambda.s3_copy', autospec=True)
    def test_run(self, fake_s3_copy, fake_update_s3_index, fake_get_obj_metadata, fake_s3_delete):
        """
        This test is currently mainly just to confirm all the right parameters
        are passed.  Basically, functioning as a "compiler".
        """
        event = {
            'Records': [
                {
                    's3': {
                        'object': { 'key': '02e4578abd294eb42c5c625b6340bd59&4&4&30&0&0&8802' },
                        'bucket': { 'name': BUCKET_NAME } } 
                }],
            'region': REGION   # This is added by the lambda handler function.
        }

        fake_get_obj_metadata.return_value = { 'ingest_job': 43 }

        run(event, Context())

    @mock_s3
    @patch('lambdafcns.cuboid_import_lambda.update_s3_index', autospec=True)
    def test_run_with_s3(self, mock_update_s3_index):
        """
        Test with moto's mock s3.
        """
        context = Context()
        names = AWSNames.from_lambda(context.function_name)
        s3 = boto3.resource('s3', region_name=REGION)
        bucket = s3.create_bucket(Bucket=BUCKET_NAME)
        target_bucket = s3.create_bucket(Bucket=names.cuboid_bucket)
        key = '02e4578abd294eb42c5c625b6340bd59&4&4&30&0&0&8802'
        data = 'my_cuboid'.encode()
        ingest_job = '29'
        metadata = json.dumps({'ingest_job': ingest_job}, separators=(',', ':'))
        bucket.put_object(Key=key, Body=data, Metadata={'metadata': metadata})

        event = {
            'Records': [
                {
                    's3': {
                        'object': { 'key': key },
                        'bucket': { 'name': BUCKET_NAME } } 
                }],
            'region': REGION   # This is added by the lambda handler function.
        }

        run(event, context)

        # Check that function to update S3 index called properly.
        obj_store_cfg = get_object_store_cfg(names.cuboid_bucket, names.s3_index)
        mock_update_s3_index.assert_called_with(obj_store_cfg, key, ingest_job)
        
        # Check that S3 object copied to target bucket.
        versioned_key = '{}&0'.format(key)
        with io.BytesIO() as f:
            target_bucket.download_fileobj(versioned_key, f)
            actual = f.getvalue()
            self.assertEqual(data, actual)

        # Confirm S3 object marked for deletion in source bucket.
        client = boto3.client('s3', region_name=event['region'])
        actual_obj = client.get_object_tagging(Bucket=BUCKET_NAME, Key=key)
        self.assertEqual(TAG_DELETE_KEY, actual_obj['TagSet'][0]['Key'])
        self.assertEqual(TAG_DELETE_VALUE, actual_obj['TagSet'][0]['Value'])
