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

"""
Lambda that removes the entry from the DynamoDB tile index for a given chunk.
This happens after a chunk is successfully placed in the cuboid bucket as
Boss cuboids.

Does not handle errors.  Assumed that this lambda will be invoked
asynchronously and caller will rely on AWS' automatic retries (3).

event: {
    'chunk_key': (str) key identifying entry in tile index.
    'task_id': (int) Id of the ingest job that the chunk belongs to.
    'region': (str) AWS region tile index lives in.
    'tile_index': (str) Name of the DynamoDB table that is the tile index.
}
"""

import boto3


def handler(event, context):
    """
    Entry point for the lambda function.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (dict): Standard lambda context parameter (ignored).

    Raises:
    """
    resp = delete_entry(event)

def delete_entry(event):
    chunk_key = event['chunk_key']
    task_id = event['task_id']
    key = {'chunk_key': {'S':chunk_key}, 'task_id': {'N': str(task_id)}}

    dynamo = boto3.client('dynamodb', region_name=event['region'])
    resp = dynamo.delete_item(Key=key, TableName=event['tile_index'])
