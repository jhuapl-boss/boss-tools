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

# lambdafcns is a symbolic link to boss-tools/lambda.  Since lambda is a
# reserved word, this allows importing lambda functions without
# updating scripts responsible for deploying the lambda code.

from lambdafcns.delete_tile_objs_lambda import handler, delete_keys, DeleteFailed
import botocore
import boto3
import moto
import unittest
from unittest.mock import patch

class TestDeleteTileObjsLambda(unittest.TestCase):

    # moto issue here: https://github.com/spulec/moto/issues/1581
    @unittest.skip('moto 1.3.3 incorrect reports an error here')
    @moto.mock_s3
    def test_delete_non_existent_keys(self):
        keys = ['foo', 'xyz']
        event = {
            'region': 'us-east-1',
            'bucket': 'test.bucket.boss',
            'tile_key_list': keys
        }
        context = None

        s3 = boto3.resource('s3', region_name=event['region'])
        s3.create_bucket(Bucket=event['bucket'])

        handler(event, context)

    @moto.mock_s3
    def test_delete_raises_on_bad_bucket_name(self):
        keys = ['tile1', 'tile2']
        event = {
            'region': 'us-east-1',
            'bucket': 'bad.bucket.name',
            'tile_key_list': keys
        }
        context = None

        with self.assertRaises(botocore.exceptions.ClientError):
            handler(event, context)

    @patch('lambdafcns.delete_tile_objs_lambda.delete_keys')
    def test_delete_raises_on_error_in_resp(self, fake_delete):
        keys = ['tile1', 'tile2']
        event = {
            'region': 'us-east-1',
            'bucket': 'test.bucket.boss',
            'tile_key_list': keys
        }
        context = None

        fake_delete.return_value = {
            'Errors': [{'Key': 'tile1'}],
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }

        with self.assertRaises(DeleteFailed):
            handler(event, context)
