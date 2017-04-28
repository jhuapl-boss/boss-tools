import sys
import json
import logging
import blosc
import boto3
import hashlib
import numpy as np
from PIL import Image
from scipy.ndimage.interpolation import zoom
from spdb.c_lib.ndtype import CUBOIDSIZE

from multidimensional import XYZ, Buffer
from multidimensional import range as xyz_range

log = logging.getLogger()
log.setLevel(logging.DEBUG)

np_types = {
    'uint64': np.uint64,
    'uint16': np.uint16,
    'uint8': np.uint8,
}

# BOSS Key creation functions / classes
def HashedKey(*args, version = None):
    """
    Args:
        collection_id
        experiment_id
        channel_id
        resolution
        time_sample
        morton (str): Morton ID of cube

    Keyword Args:
        version : Optional Object version
    """
    key = '&'.join([str(arg) for arg in args if arg is not None])
    digest = hashlib.md5(key.encode()).hexdigest()
    key = '{}&{}'.format(digest, key)
    if version is not None:
        key = '{}&{}'.format(key, version)
    return key

class S3Bucket(object):
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
        except:
            raise Exception("No Such Key")

        self._check_error(resp, "reading")

        data = resp['Body'].read()
        return data

    def put(self, key, data):
        resp = self.s3.put_object(Key = key,
                                  Body = data,
                                  Bucket = self.bucket)

        self._check_error(resp, "writing")

class S3IndexKey(dict):
    def __init__(self, obj_key, version=0, job_hash=None, job_range=None):
        super().__init__()
        self['object-key'] = {'S': obj_key}
        self['version-node'] = {'N': str(version)}

        if job_hash is not None:
            self['ingest-job-hash'] = {'S': str(job_hash)}

        if job_range is not None:
            self['ingest-job-range'] = {'S': job_range}

class IdIndexKey(dict):
    def __init__(self, chan_key, version=0):
        super().__init__()
        self['channel-id-key'] = {'S': chan_key}
        self['version'] = {'N': str(version)}

class DynamoDBTable(object):
    def __init__(self, table):
        self.table = table
        self.ddb = boto3.client('dynamodb')

    def _check_error(self, resp, action):
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception("Error {} index information to/from/in DynamoDB".format(action))

    def put(self, item):
        try:
            self.ddb.put_item(TableName = self.table,
                              Item = item,
                              ReturnConsumedCapacity = 'NONE',
                              ReturnItemCollectionMetrics = 'NONE')
        except:
            raise Exception("Error adding item to DynamoDB Table")

    def update_ids(self, key, ids):
        resp = self.ddb.update_item(TableName = self.table,
                                    Key = key,
                                    UpdateExpression='ADD #idset :ids',
                                    ExpressionAttributeNames={'#idset': 'id-set'},
                                    ExpressionAttributeValues={':ids': {'NS': ids}},
                                    ReturnConsumedCapacity='NONE')

        self._check_error(resp, 'updating')

    def update_id(self, key, obj_key):
        resp = self.ddb.update_item(TableName = self.table,
                                    Key = key,
                                    UpdateExpression='ADD #cuboidset :objkey',
                                    ExpressionAttributeNames={'#cuboidset': 'cuboid-set'},
                                    ExpressionAttributeValues={':objkey': {'SS': [obj_key]}},
                                    ReturnConsumedCapacity='NONE')

        self._check_error(resp, 'updating')

    def exists(self, key):
        resp = self.ddb.get_item(TableName = self.table,
                                 Key = key,
                                 ConsistentRead=True,
                                 ReturnConsumedCapacity='NONE')

        return 'Item' in resp

# ACTUAL Lambda method
def downsample_volume(args, target, step, dim, use_iso_key):
    """
    Args:
        args {
            collection_id (int)
            experiment_id (int)
            channel_id (int)
            annotation_channel (bool)
            data_type (str) 'uint8' | 'uint16' | 'uint64'

            s3_bucket (URL)
            s3_index (URL)
            id_index (URL)

            resolution (int) The resolution to downsample. Creates resolution + 1

            type (str) 'isotropic' | 'anisotropic'
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both
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
    s3_index = DynamoDBTable(args['s3_index'])
    id_index = DynamoDBTable(args['id_index'])

    # Download all of the cubes that will be downsamples
    volume = Buffer.zeros(dim * step, dtype=np_types[data_type], order='C')
    volume.dim = dim
    volume.cubes = step

    volume_empty = True
    for offset in xyz_range(step):
        cube = target + offset
        try:
            obj_key = HashedKey(parent_iso, col_id, exp_id, chan_id, resolution, t, cube.morton, version=version)
            data = s3.get(obj_key)
            data = blosc.decompress(data)

            # DP ???: Check to see if the buffer is all zeros?
            data = Buffer.frombuffer(data, dtype=np_types[data_type])
            data.resize(dim)

            #log.debug("Downloaded cube {}".format(cube))
            volume[offset * dim: (offset + 1) * dim] = data
            volume_empty = False
        except Exception as e: # TODO: Create custom exception for S3 download
            #log.exception("Problem downloading cubes {}".format(cube))
            #log.debug("No cube at {}".format(cube))
            pass

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
                             '{}&{}&{}&{}'.format(exp_id, chan_id, resolution + 1, ingest_job))
        s3_index.put(idx_key)

def downsample_cube(volume, cube, is_annotation):
    """
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
        image_type = None
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

        if is_annotation:
            cube[z, :, :] = zoom(slice, 0.5, np.uint64, order=0, mode='nearest')
        else:
            image = Image.frombuffer(image_type,
                                     (volume.shape.x, volume.shape.y),
                                     slice.flatten(),
                                     'raw',
                                     image_type,
                                     0, 1)

            cube[z, :, :] = Buffer.asarray(image.resize((cube.shape.x, cube.shape.y), Image.BILINEAR))

def handler(args, context):
    def convert(key):
        args[key] = XYZ(*args[key])

    convert('target')
    convert('step')
    convert('dim')

    downsample_volume(args['args'], args['target'], args['step'], args['dim'], args['use_iso_key'])

