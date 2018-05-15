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

from lambdafcns.delete_tile_index_entry_lambda import handler
import botocore
import boto3
import moto
import unittest
from unittest.mock import patch

class TestDeleteTileIndexEntryLambda(unittest.TestCase):
    def setUp(self):
        self.table_name = 'test.index.boss'
        self.region_name = 'us-east-1'
        table_params = self.get_tile_schema()
        self.mock_dynamo = moto.mock_dynamodb2()
        self.mock_dynamo.start()

        self.dynamo = boto3.client('dynamodb', region_name=self.region_name)
        self.dynamo.create_table(TableName=self.table_name, **table_params)

    def tearDown(self):
        self.mock_dynamo.stop()

    def test_delete(self):
        fake_key = 'fakekey'
        task_id = 47

        event = {
            'region': self.region_name,
            'tile_index': self.table_name,
            'chunk_key': fake_key,
            'task_id': task_id
        }
        context = None

        self.dynamo.put_item(
            TableName=self.table_name,
            Item={
                'chunk_key': {'S': fake_key},
                'task_id': {'N': str(task_id)}
            }
        )

        handler(event, context)

    def test_delete_key_doesnt_exist(self):
        fake_key = 'nonexistantkey'
        task_id = 97

        event = {
            'region': self.region_name,
            'tile_index': self.table_name,
            'chunk_key': fake_key,
            'task_id': task_id
        }
        context = None

        handler(event, context)

    def get_tile_schema(self):
        """
        Tile index schema from ndingest/nddynamo/schemas/boss_tile_index.json.

        Not loading directly because we don't know where this repo will live
        on each dev's machine.

        Returns:
            (dict)
        """
        return {
          "KeySchema": [
            {
              "AttributeName": "chunk_key",
              "KeyType": "HASH"
            },
            {
                "AttributeName": "task_id",
                "KeyType": "RANGE"
            }
          ],
          "AttributeDefinitions": [
            {
              "AttributeName": "chunk_key",
              "AttributeType": "S"
            },
            {
              "AttributeName": "task_id",
              "AttributeType": "N"
            }
          ],
          "GlobalSecondaryIndexes": [
            {
              "IndexName": "task_index",
              "KeySchema": [
                {
                  "AttributeName": "task_id",
                  "KeyType": "HASH"
                }
              ],
              "Projection": {
                "ProjectionType": "ALL"
              },
              "ProvisionedThroughput": {
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10
              }
            }
          ],
          "ProvisionedThroughput": {
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
          }
        }
