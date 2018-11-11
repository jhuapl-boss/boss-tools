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
Mark for deletion, tiles associated with a single chunk, from the tile bucket.
Actual deletion will be handled by a bucket life cycle policy that looks for
the delete tag.

Does not handle errors.  Assumed that this lambda will be invoked
asynchronously and caller will rely on AWS' automatic retries (3).

event: {
    tile_key_list (list[str]): list of S3 keys to delete from bucket
    region (str): AWS region bucket lives in
    bucket (str): Name of tile bucket
}
"""

from bossnames.bucket_object_tags import TAG_DELETE_KEY, TAG_DELETE_VALUE
import boto3

def handler(event, context):
    """
    Entry point for the lambda function.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (dict): Standard lambda context parameter (ignored).
    """
    mark_keys_for_deletion(event)

def mark_keys_for_deletion(event):
    """
    Makes S3 API call that tags multiple keys with the delete tag.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
    """
    s3 = boto3.client('s3', region_name=event['region'])
    for key in event['tile_key_list']:
        s3.put_object_tagging(
            Bucket=event['bucket'], Key=key, Tagging={
                'TagSet': [{ 'Key': TAG_DELETE_KEY, 'Value': TAG_DELETE_VALUE }]
            }
        )
