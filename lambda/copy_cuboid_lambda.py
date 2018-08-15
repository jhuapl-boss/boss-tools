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
event: {
    's3_key': (str) S3 object key of source cuboid.
    'translate': (list[x, y, z]) Translate cuboid's position by these deltas.
                                 Must be a multiple of the cuboid's dimensions.
    'region': (str) AWS region resources exist in.
    'lookup_key': (str) coll_id&exp_id&chan_id (DB ids with separating ampersands).
    'ingest_job': (int) Id of ingest job created for this copy.
    'version': (int) 0 - should always be 0 until versioning actually implementd.
    'object_store_config': (dict) Initialization data for AWSObjectStore.
    {
        'id_count_table': '...',
        'page_in_lambda_function': '...',
        'id_index_table': '...',
        'cuboid_bucket': '...',
        's3_index_table': '...',
        'page_out_lambda_function': '...',
        's3_flush_queue': '...'
    }
}
"""

import boto3
import botocore
import hashlib
import logging
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.c_lib.ndlib import XYZMorton, MortonXYZ
from spdb.spatialdb.object import AWSObjectStore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Entry point for the lambda function.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
        context (dict): Standard lambda context parameter (ignored).
    """
    copy_cuboid(event)

def copy_cuboid(event):
    """
    Main worker function for this lambda.

    Args:
        event (dict): Lambda input parameters.  See module level comments.
    """
    validate_translation(event['translate'])
    new_key = generate_new_key(event['s3_key'], event['translate'], event['lookup_key'])

    bucket = event['object_store_config']['cuboid_bucket']
    source = { 'Bucket': bucket, 'Key': '{}&{}'.format(event['s3_key'], event['version']) }

    logger.info('Copying {} to {}'.format(event['s3_key'], new_key))

    if not s3_copy(bucket, source, new_key, event['region']):
        # Key not present, so don't add to index.
        return

    update_s3_index(event['object_store_config'], new_key, event['ingest_job'])

def s3_copy(bucket, source, key, region):
    """
    Executes S3 copy operation.

    Args:
        bucket (str): Name of S3 bucket.
        source (dict): Identify source cuboid, keys: 'Bucket', 'Key'.
        key (str): Copy destination S3 object key.
        region (str): AWS region.

    Returns:
        (bool): False if source not found.
    """

    # Append version (always 0 particularly since we're copying to a new
    # channel.
    versioned_key = '{}&0'.format(key)

    s3 = boto3.client('s3', region_name=region)
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource=source,
            Key=versioned_key
        )
    except s3.exceptions.NoSuchKey as ex:
        logger.warn('Key does not exist in S3 - probably black tile.')
        return False

    return True

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

def validate_translation(translate):
    """
    Check that translation is cuboid aligned (multiple of cuboid's dimensions.

    Args:
        translate (list[x, y, z]):

    Raises:
        (ValueError): if at least one translation axis is not cuboid aligned.
    """
    if translate[0] % CUBOIDSIZE[0][0] != 0:
        raise ValueError(
            "X translation ({}) is not a multiple of the cuboid's X size ({})".format(
            translate[0], CUBOIDSIZE[0][0]))

    if translate[1] % CUBOIDSIZE[0][1] != 0:
        raise ValueError(
            "Y translation ({}) is not a multiple of the cuboid's Y size ({})".format(
            translate[1], CUBOIDSIZE[0][1]))

    if translate[2] % CUBOIDSIZE[0][2] != 0:
        raise ValueError(
            "Z translation ({}) is not a multiple of the cuboid's Z size ({})".format(
            translate[2], CUBOIDSIZE[0][2]))

def generate_new_key(obj_key, translate, lookup_key):
    """
    Generate a new object key where the copied cuboid will live.

    Args:
        obj_key (str): Source cuboid's S3 object key.
        translate (list[x, y, z]): Translate cuboid's position by these deltas.
        lookup_key (str): coll_id&exp_id&chan_id (DB ids with separating ampersands).

    Returns:
        (str): Object key for cuboid copy destination.

    Raises:
        (ValueError): Only cuboids at resolution 0 may be copied, currently.
    """
    parts = AWSObjectStore.get_object_key_parts(obj_key)
    if int(parts.resolution) != 0:
        raise ValueError('Copying non-zero resolutions not currently supported.')

    orig_x, orig_y, orig_z = MortonXYZ(parts.morton_id)

    new_x = orig_x + translate[0] / CUBOIDSIZE[0][0]
    new_y = orig_y + translate[1] / CUBOIDSIZE[0][1]
    new_z = orig_z + translate[2] / CUBOIDSIZE[0][2]

    new_morton = XYZMorton([new_x, new_y, new_z])

    base_key = '{}&{}&{}&{}'.format(lookup_key, parts.resolution, parts.time_sample, new_morton)

    hash_str = hashlib.md5(base_key.encode()).hexdigest()

    return "{}&{}".format(hash_str, base_key)
