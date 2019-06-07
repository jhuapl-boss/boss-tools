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

from lambdafcns.delete_tile_objs_lambda import handler
from bossnames.bucket_object_tags import TAG_DELETE_KEY, TAG_DELETE_VALUE
import botocore
import boto3
import moto
import unittest

class TestDeleteTileObjsLambda(unittest.TestCase):

    # moto issue here: https://github.com/spulec/moto/issues/1581
    #@unittest.skip('moto 1.3.3 incorrect reports an error here')
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

        client = boto3.client('s3', region_name=event['region'])
        with self.assertRaises(client.exceptions.NoSuchKey):
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

    @moto.mock_s3
    def test_objects_marked_for_deletion(self):
        keys = ['tilex', 'tiley']
        event = {
            'region': 'us-east-1',
            'bucket': 'test.bucket.boss',
            'tile_key_list': keys
        }

        s3 = boto3.resource('s3', region_name=event['region'])
        s3.create_bucket(Bucket=event['bucket'])

        obj1 = s3.Object(event['bucket'], keys[0])
        obj1.put(Body=b'obj1')

        obj2 = s3.Object(event['bucket'], keys[1])
        obj2.put(Body=b'obj2')

        context = None

        handler(event, context)

        client = boto3.client('s3', region_name=event['region'])
        actual_obj1 = client.get_object_tagging(Bucket=event['bucket'], Key=keys[0])
        actual_obj2 = client.get_object_tagging(Bucket=event['bucket'], Key=keys[1])

        self.assertEqual(TAG_DELETE_KEY, actual_obj1['TagSet'][0]['Key'])
        self.assertEqual(TAG_DELETE_VALUE, actual_obj1['TagSet'][0]['Value'])

        self.assertEqual(TAG_DELETE_KEY, actual_obj2['TagSet'][0]['Key'])
        self.assertEqual(TAG_DELETE_VALUE, actual_obj2['TagSet'][0]['Value'])

