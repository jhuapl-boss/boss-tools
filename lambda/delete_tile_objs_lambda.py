# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Remove tiles associated with a single chunk from the tile bucket.

Does not handle errors.  Assumed that this lambda will be invoked
asynchronously and caller will rely on AWS' automatic retries (3).

event: {
    tile_key_list (list[str]): list of S3 keys to delete from bucket
    region (str): AWS region bucket lives in
    bucket (str): Name of tile bucket
}
"""

import boto3


class DeleteFailed(Exception):
    """
    Raised when at least one key passed to delete_objects() failed to delete.
    """
    def __init__(self, msg):
        super().__init__(msg)

def handler(event, context):
    """
    Entry point for the lambda function.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (dict): Standard lambda context parameter (ignored).

    Raises:
        (DeleteFailed): Raised when failed to delete at least one key.
    """
    resp = delete_keys(event)
    if 'Errors' in resp:
        raise DeleteFailed('Error deleting: {}'.format(resp['Errors']))

def delete_keys(event):
    """
    Makes S3 API call to delete multiple keys from the given bucket.

    Args:
        event (dict): Lambda input parameters.  See module level comments.

    Returns:
        (dict): Response dictionary.
    """
    s3 = boto3.client('s3', region_name=event['region'])
    keys = [{'Key': key} for key in event['tile_key_list']]
    resp = s3.delete_objects(
        Bucket=event['bucket'],
        Delete={
            'Objects': keys,
            'Quiet':True
        }
    )
    print(resp)

    return resp
