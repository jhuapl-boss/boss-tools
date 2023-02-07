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

import importlib
# from lambda import downsample_volume as dsv
dsv = importlib.import_module("lambda.downsample_volume")

import unittest
from unittest.mock import patch

from bossutils.multidimensional import XYZ
import numpy as np
import blosc

import boto3
from moto import mock_s3, mock_sqs, mock_dynamodb2

# Downsample Volume Lambda handler arguments
args = {
    'bucket_size': 1,
    'cubes_arn': 'cubes_arn',

    'args': {
        'collection_id': 1,
        'experiment_id': 1,
        'channel_id': 1,
        'annotation_channel': False,
        'data_type': 'uint8',

        's3_bucket': 's3_bucket',
        's3_index': 's3_index',

        'resolution': 0,

        'type': 'anisotropic',
        'iso_resolution': 4,

        'aws_region': 'us-east-1',
    },
    'step': [2,2,1],
    'dim': [512,512,16],
    'use_iso_flag': False,
}

# Since there is no default for the region_name, set one
boto3.setup_default_session(region_name = 'us-east-1')

class TestDownsampleVolumeLambda(unittest.TestCase):

    @patch('lambda.downsample_volume.S3DynamoDBTable', autospec=True)
    @patch('lambda.downsample_volume.S3Bucket', autospec=True)
    @patch('blosc.decompress', autospec=True)
    def test_downsample_volume(self, fake_decompress, fake_s3, fake_s3_ind):
        """
        Just execute the majority of the code in downsample_volume() to catch
        typos and other errors that might show up at runtime.
        """
        fake_s3.get.return_value = None
        fake_decompress.return_value = np.random.randint(
            0, 256, (16, 512, 512), dtype='uint64')

        args = dict(
            collection_id=1,
            experiment_id=2,
            channel_id=3,
            annotation_channel=True,
            data_type='uint64',
            s3_bucket='testBucket.example.com',
            s3_index='s3index.example.com',
            resolution=0,
            type='isotropic',
            iso_resolution=4,
            aws_region='us-east-1'
        )
        target = XYZ(0, 0, 0)
        step = XYZ(2, 2, 2)
        dim = XYZ(512, 512, 16)
        use_iso_key = True

        dsv.downsample_volume(args, target, step, dim, use_iso_key)

    @mock_sqs()
    @mock_s3()
    def test_empty_volume(self):
        # Create cubes_arn and populate with one target cube
        sqs = boto3.client('sqs')
        sqs.create_queue(QueueName = 'cubes_arn')
        sqs.send_message(QueueUrl = 'cubes_arn',
                         MessageBody = '[0,0,0]')

        # Create the s3_bucket Bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket = 's3_bucket')

        dsv.handler(args, None)

        # TODO check s3 and verify no cubes were added

    @mock_dynamodb2()
    @mock_sqs()
    @mock_s3()
    def test_full_volume(self):
        # Create cubes_arn and populate with one target cube
        sqs = boto3.client('sqs')
        sqs.create_queue(QueueName = 'cubes_arn')
        sqs.send_message(QueueUrl = 'cubes_arn',
                         MessageBody = '[0,0,0]')

        # Create s3_index table
        ddb = boto3.client('dynamodb')
        ddb.create_table(TableName = 's3_index',
                         AttributeDefinitions = [
                            { "AttributeName": "object-key", "AttributeType": "S" },
                            { "AttributeName": "version-node", "AttributeType": "N" },
                            { "AttributeName": "lookup-key", "AttributeType": "S" }
                         ],
                         KeySchema = [
                            { "AttributeName": "object-key", "KeyType": "HASH" },
                            { "AttributeName": "version-node", "KeyType": "RANGE" }
                         ],
                         GlobalSecondaryIndexes = [
                            { "IndexName": "lookup-key-index",
                              "KeySchema": [
                                  { "AttributeName": "lookup-key", "KeyType": "HASH" }
                              ],
                              "Projection": { "ProjectionType": "KEYS_ONLY" },
                              "ProvisionedThroughput": {
                                  "ReadCapacityUnits": 15,
                                  "WriteCapacityUnits": 15
                              }
                            }
                         ],
                         ProvisionedThroughput = {
                            "ReadCapacityUnits": 15,
                            "WriteCapacityUnits": 15
                         })

        # Create the s3_bucket Bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket = 's3_bucket')

        # Create cube of data
        data = np.zeros([16,512,512], dtype=np.uint8, order='C')
        data = blosc.compress(data, typesize=8)

        # Put cube data for the target cubes
        for key in [dsv.HashedKey(None, 1,1,1,0,0,0,version=0),
                    dsv.HashedKey(None, 1,1,1,0,0,1,version=0),
                    dsv.HashedKey(None, 1,1,1,0,0,2,version=0),
                    dsv.HashedKey(None, 1,1,1,0,0,3,version=0)
                   ]:
            s3.put_object(Bucket = 's3_bucket',
                          Key = key,
                          Body = data)

        dsv.handler(args, None)

        # TODO check s3 and make sure cubes were added
        # TODO check dynamodb and make sure index enteries were added
