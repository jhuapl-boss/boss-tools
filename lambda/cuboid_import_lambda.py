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
This lambda is invoked when a cuboid is uploaded to the ingest bucket.  This
bucket is a temporary holding bucket for cuboids during volumetric ingest.
The cuboid is copied to the standard S3 cuboid bucket and the S3 index table
is updated with the new cuboid's key.

This lambda is referenced by the key 'cuboid_import_lambda' in names.py.

The event parameter passed into the lambda should be provided by the ingest
bucket's trigger.

event: {
    "Records": [
        {
            "s3": {
                "object": { "key": (str): S3 object key },
                "bucket": { "name": (str): bucket name }
            } 
        }
    ]
}
"""

from bossnames.bucket_object_tags import TAG_DELETE_KEY, TAG_DELETE_VALUE
from bossnames.names import AWSNames
import boto3
import json
import logging
import os
from spdb.spatialdb.object import AWSObjectStore
import urllib

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

def handler(event, context):
    """
    Entry point for the lambda function.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (dict): Standard lambda context parameter (ignored).
    """

    # Determine region lambda is executing in.
    event['region'] = os.environ['AWS_REGION']
    run(event, context)

def run(event, context):
    """
    Main worker function for this lambda.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (Context): Lambda Context object.

    Raises:
        (ValueError): if there is no exactly one Record in event.
    """
    logger.info(event)

    num_records = len(event['Records'])
    if num_records != 1:
        raise ValueError('S3 trigger event sent {} records.'.format(num_records))

    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    bucket = event['Records'][0]['s3']['bucket']['name']

    names = AWSNames.create_from_lambda_name(context.function_name)
    if bucket != names.ingest_bucket:
        raise ValueError('Error: event fired from unexpected bucket: {}'.format(bucket))

    source = { 'Bucket': bucket, 'Key': key }
    target_bucket = names.cuboid_bucket
    region = event['region']

    metadata = get_object_metadata(bucket, key, region)
    logger.info('Metadata: {}'.format(metadata))
    ingest_job = metadata['ingest_job']

    logger.info('Copying {}'.format(key))
    s3_copy(target_bucket, source, region)

    logger.info('Updating S3 index table')
    s3_index = names.s3_index
    update_s3_index(get_object_store_cfg(target_bucket, s3_index), key, ingest_job)

    logger.info('Cuboid in ingest bucket marked for deletion')
    s3_mark_for_deletion(bucket, key, region)

def s3_copy(target_bucket, source, region):
    """
    Executes S3 copy operation.

    Args:
        target_bucket (str): Name of S3 bucket.
        source (dict): Identify source cuboid, keys: 'Bucket', 'Key'.
        region (str): AWS region.
    """

    # Append version (always 0 particularly since we're copying to a new
    # channel.
    versioned_key = '{}&0'.format(source['Key'])

    s3 = boto3.client('s3', region_name=region)
    s3.copy_object(
        Bucket=target_bucket,
        CopySource=source,
        Key=versioned_key
    )

def s3_mark_for_deletion(bucket, key, region):
    """
    Delete the S3 object.

    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        region (str): AWS region.
    """
    s3 = boto3.client('s3', region_name=region)
    s3.put_object_tagging(
        Bucket=bucket, Key=key, Tagging={
            'TagSet': [{ 'Key': TAG_DELETE_KEY, 'Value': TAG_DELETE_VALUE }]
        }
    )

def get_object_metadata(bucket, key, region):
    """
    Get the S3 object's metadata.

    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        region (str): AWS region.

    Returns:
        (dict): Expect key: ingest_job
    """
    s3 = boto3.resource('s3', region_name=region)
    obj = s3.Object(bucket, key)
    return json.loads(obj.metadata['metadata'])
    
def get_object_store_cfg(bucket, s3_index):
    """
    Create the bare minimum to successfully instantiate AWSObjectStore.

    Args:
        bucket (str): S3 bucket name.
        s3_index (str): Name of the DynamoDB S3 index table.

    Returns:
        (dict):
    """
    return {
        'id_count_table': 'foo',
        'page_in_lambda_function': 'foo',
        'id_index_table': 'foo',
        'cuboid_bucket': bucket,
        's3_index_table': s3_index,
        'page_out_lambda_function': 'foo',
        's3_flush_queue': 'foo'
    }

def update_s3_index(obj_store_init, key, ingest_job):
    """
    Updates DynamoDB S3 index with new cuboid.

    Args:
        obj_store_init (dict): See top level comment for description.
        key (str): S3 object key of new cuboid.
        ingest_job (int): Ingest job id to associate with this copy.
    """
    obj_store = AWSObjectStore(obj_store_init)
    obj_store.add_cuboid_to_index(key, ingest_job=ingest_job)
