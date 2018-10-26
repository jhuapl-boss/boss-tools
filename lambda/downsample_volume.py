import sys
import json
import logging
import blosc
import boto3
import hashlib
import numpy as np
from PIL import Image
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.c_lib import ndlib
from spdb.spatialdb import AWSObjectStore
from spdb.spatialdb.object import ObjectIndices
from random import randrange
from botocore.exceptions import ClientError

from bossutils.multidimensional import XYZ, Buffer
from bossutils.multidimensional import range as xyz_range

log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(log_handler)


LOOKUP_KEY_MAX_N = 100 # DP NOTE: Taken from spdb.spatialdb.object

EXCEPTIONS_NOT_RELATED_TO_KEY = ('ProvisionedThroughputExceededException', 'ThrottlingException')


np_types = {
    'uint64': np.uint64,
    'uint16': np.uint16,
    'uint8': np.uint8,
}

#### Helper functions and classes ####

def HashedKey(*args, version = None):
    """ BOSS Key creation function

    Takes a list of different key string elements, joins them with the '&' char,
    and prepends the MD5 hash of the key to the key.

    Args (Common usage):
        collection_id
        experiment_id
        channel_id
        resolution
        time_sample
        morton (str): Morton ID of cube

    Keyword Args:
        version : Optional Object version, not part of the hashed value
    """
    key = '&'.join([str(arg) for arg in args if arg is not None])
    digest = hashlib.md5(key.encode()).hexdigest()
    key = '{}&{}'.format(digest, key)
    if version is not None:
        key = '{}&{}'.format(key, version)
    return key

class S3Bucket(object):
    """Wrapper for calls to S3

    Wraps boto3 calls to upload and download data from S3
    """
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3 = boto3.client('s3')

    def _check_error(self, resp, action):
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception("Error {} cuboid to/from S3".format(action))

    def get(self, key):
        try:
            resp = self.s3.get_object(Key = key,
                                      Bucket = self.bucket)
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                # Cube doesn't exist
                return None
            if e.response['Error']['Code'] not in EXCEPTIONS_NOT_RELATED_TO_KEY:
                print("S3 key in get_object error: {}".format(key))
            raise

        data = resp['Body'].read()
        return data

    def put(self, key, data):
        resp = self.s3.put_object(Key = key,
                                  Body = data,
                                  Bucket = self.bucket)

        self._check_error(resp, "writing")

class S3IndexKey(dict):
    """Key object for DynamoDB S3 Index table
    
    May also contain optional attributes.

    Attributes:
        obj_key (str): HashedKey of the stored data
        version (int): Reserved for future use.
        job_hash: Identifies owning collection.
        job_range: Identifies experiment, channel, resolution, and ingest job id.
        lookup_key (str): Identifies owning channel (used by lookup-key-index).
    """
    def __init__(self, obj_key, version=0, job_hash=None, job_range=None, lookup_key=None):
        super().__init__()
        self['object-key'] = {'S': obj_key}
        self['version-node'] = {'N': str(version)}

        if job_hash is not None:
            self['ingest-job-hash'] = {'S': str(job_hash)}

        if job_range is not None:
            self['ingest-job-range'] = {'S': job_range}

        if lookup_key is not None:
            self['lookup-key'] = {'S': lookup_key}

class S3DynamoDBTable(object):
    """Wrapper for calls to DynamoDB

    Wraps boto3 calls to create and update DynamoDB entries.

    Supports updates for S3 Index tables
    """
    def __init__(self, table, region):
        self.table = table
        self.ddb = boto3.client('dynamodb', region_name=region)

    def put(self, item):
            self.ddb.put_item(TableName = self.table,
                              Item = item,
                              ReturnConsumedCapacity = 'NONE',
                              ReturnItemCollectionMetrics = 'NONE')

    def exists(self, key):
        resp = self.ddb.get_item(TableName = self.table,
                                 Key = key,
                                 ConsistentRead=True,
                                 ReturnConsumedCapacity='NONE')

        return 'Item' in resp


#### Main lambda logic ####

def downsample_volume(args, target, step, dim, use_iso_key):
    """Downsample a volume into a single cube

    Download `step` cubes from S3, downsample them into a single cube, upload
    to S3 and update the S3 index for the new cube.

    Args:
        args {
            collection_id (int)
            experiment_id (int)
            channel_id (int)
            annotation_channel (bool)
            data_type (str) 'uint8' | 'uint16' | 'uint64'

            s3_bucket (URL)
            s3_index (str)

            resolution (int) The resolution to downsample. Creates resolution + 1

            type (str) 'isotropic' | 'anisotropic'
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both

            aws_region (str) AWS region to run in such as us-east-1
        }

        target (XYZ) : Corner of volume to downsample
        step (XYZ) : Extent of the volume to downsample
        dim (XYZ) : Dimensions of a single cube
        use_iso_key (boolean) : If the BOSS keys should include an 'ISO=' flag
    """
    log.debug("Downsampling {}".format(target))
    # Hard coded values
    version = 0
    t = 0
    dim_t = 1

    iso = 'ISO' if use_iso_key else None

    # If anisotropic and resolution is when neariso is reached, the first
    # isotropic downsample needs to use the anisotropic data. Future isotropic
    # downsamples will use the previous isotropic data.
    parent_iso = None if args['resolution'] == args['iso_resolution'] else iso

    col_id = args['collection_id']
    exp_id = args['experiment_id']
    chan_id = args['channel_id']
    data_type = args['data_type']
    annotation_chan = args['annotation_channel']

    resolution = args['resolution']

    s3 = S3Bucket(args['s3_bucket'])
    s3_index = S3DynamoDBTable(args['s3_index'], args['aws_region'])

    # Download all of the cubes that will be downsamples
    volume = Buffer.zeros(dim * step, dtype=np_types[data_type], order='C')
    volume.dim = dim
    volume.cubes = step

    volume_empty = True # abort if the volume doesn't exist in S3
    for offset in xyz_range(step):
        if args.get('test'):
            # Enable Test Mode
            # This is where the cubes downsamples are all taken from 0/0/0
            # so that the entire frame doesn't have to be populated to test
            # the code paths that downsample cubes
            cube = offset # use target 0/0/0
        else:
            cube = target + offset

        obj_key = HashedKey(parent_iso, col_id, exp_id, chan_id, resolution, t, cube.morton, version=version)
        data = s3.get(obj_key)
        if data:
            data = blosc.decompress(data)

            # DP ???: Check to see if the buffer is all zeros?
            data = Buffer.frombuffer(data, dtype=np_types[data_type])
            data.resize(dim)

            #log.debug("Downloaded cube {}".format(cube))
            volume[offset * dim: (offset + 1) * dim] = data
            volume_empty = False

    if volume_empty:
        log.debug("Completely empty volume, not downsampling")
        return

    # Create downsampled cube
    new_dim = XYZ(*CUBOIDSIZE[resolution + 1])
    cube =  Buffer.zeros(new_dim, dtype=np_types[data_type], order='C')
    cube.dim = new_dim
    cube.cubes = XYZ(1,1,1)

    downsample_cube(volume, cube, annotation_chan)

    target = target / step # scale down the output

    # Save new cube in S3
    obj_key = HashedKey(iso, col_id, exp_id, chan_id, resolution + 1, t, target.morton, version=version)
    compressed = blosc.compress(cube, typesize=(np.dtype(cube.dtype).itemsize))
    s3.put(obj_key, compressed)

    # Update indicies
    # Same key scheme, but without the version
    obj_key = HashedKey(iso, col_id, exp_id, chan_id, resolution + 1, t, target.morton)
    # Create S3 Index if it doesn't exist
    idx_key = S3IndexKey(obj_key, version)
    if not s3_index.exists(idx_key):
        ingest_job = 0 # Valid to be 0, as posting a cutout uses 0
        idx_key = S3IndexKey(obj_key,
                             version,
                             col_id,
                             '{}&{}&{}&{}'.format(exp_id, chan_id, resolution + 1, ingest_job),
                             # Replaced call to SPDB AWSObjectStore.generate_lookup_key, as SPDB master doesn't contain this call
                             # AWSObjectStore.generate_lookup_key(col_id, exp_id, chan_id, resolution + 1)
                             '{}&{}&{}&{}&{}'.format(col_id, exp_id, chan_id, resolution + 1, randrange(LOOKUP_KEY_MAX_N)))
        s3_index.put(idx_key)


def downsample_cube(volume, cube, is_annotation):
    """Downsample the given Buffer into the target Buffer

    Note: Both volume and cube both have the following attributes
        dim (XYZ) : The dimensions of the cubes contained in the Buffer
        cubes (XYZ) : The number of cubes of size dim contained in the Buffer

        dim * cubes == Buffer.shape

    Args:
        volume (Buffer) : Raw numpy array of input cube data
        cube (Buffer) : Raw numpy array for output data
        is_annotation (boolean) : If the downsample should be an annotation downsample
    """
    #log.debug("downsample_cube({}, {}, {})".format(volume.shape, cube.shape, is_annotation))

    if is_annotation:
        # Use a C implementation to downsample each value
        ndlib.addAnnotationData_ctype(volume, cube, volume.cubes.zyx, volume.dim.zyx)
    else:
        if volume.dtype == np.uint8:
            image_type = 'L'
        elif volume.dtype == np.uint16:
            image_type = 'I;16'
        else:
            raise Exception("Unsupported type for image downsampling '{}'".format(volume.dtype))

        for z in range(cube.dim.z):
            # DP NOTE: For isotropic downsample this skips Z slices, instead of trying to merge them
            slice = volume[z * volume.cubes.z, :, :]
            image = Image.frombuffer(image_type,
                                     (volume.shape.x, volume.shape.y),
                                     slice.flatten(),
                                     'raw',
                                     image_type,
                                     0, 1)

            cube[z, :, :] = Buffer.asarray(image.resize((cube.shape.x, cube.shape.y), Image.BILINEAR))

def handler(args, context):
    def convert(args_, key):
        args_[key] = XYZ(*args_[key])

    convert(args, 'step')
    convert(args, 'dim')

    sqs = boto3.resource('sqs')
    cubes = sqs.Queue(args['cubes_arn'])

    msgs = cubes.receive_messages(MaxNumberOfMessages = args['bucket_size'],
                                  WaitTimeSeconds = 5)

    for msg in msgs:
        downsample_volume(args['args'],
                          XYZ(*json.loads(msg.body)),
                          args['step'],
                          args['dim'],
                          args['use_iso_flag'])
        msg.delete()

