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

import blosc
import hashlib
import boto3
import botocore
import numpy as np
from PIL import Image
from collections import namedtuple
from bossutils import aws, logger
from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.c_lib.ndlib import isotropicBuild_ctype
from spdb.spatialdb.imagecube import Cube, ImageCube8, ImageCube16
from spdb.spatialdb.annocube import AnnotateCube64

from multidimensional import XYZ, Buffer, XYZVolume, ceildiv
from multidimensional import range as xyz_range

log = logger.BossLogger().logger

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
        self.s3 = aws.get_session().client('s3')

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
        self.ddb = aws.get_session().client('dynamodb')

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

# ACTUAL Activities
def downsample_channel(args):
    """
    Args:
        args {
            collection_id (int)
            experiment_id (int)
            channel_id (int)
            annotation_channel (bool)
            data_type

            s3_bucket
            s3_index
            id_index

            x_start (int)
            y_start (int)
            z_start (int)

            x_stop (int)
            y_stop (int)
            z_stop (int)

            resolution (int)
            resolution_max (int)
            res_lt_max (bool)

            type (str) 'isotropic' | 'anisotropic' or boolean?
                       'isotropic' | 'slice'???
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both
                or
            x/y/z resolution (int) calculate during downsample

        }
    """

    log.debug("Downsampling resolution " + str(args['resolution']))

    resolution = args['resolution']

    dim = XYZ(*CUBOIDSIZE[resolution])
    log.debug("Cube dimensions: {}".format(dim))

    cubes_start = XYZ(args['x_start'] // dim.x,
                      args['y_start'] // dim.y,
                      args['z_start'] // dim.z)

    cubes_stop = XYZ(ceildiv(args['x_stop'], dim.x),
                     ceildiv(args['y_stop'], dim.y),
                     ceildiv(args['z_stop'], dim.z))

    steps = []
    if args['type'] == 'isotropic':
        steps.append((XYZ(2,2,2), False))
    else:
        steps.append((XYZ(2,2,1), False))
        if resolution >= args['iso_resolution']: # DP TODO: Figure out how to launch aniso iso version with mutating arguments
            steps.append((XYZ(2,2,2), True))

    for step, use_iso_flag in steps:
        log.debug("Frame corner: {}".format(cubes_start))
        log.debug("Frame extent: {}".format(cubes_stop))
        log.debug("Downsample step: {}".format(step))
        for target in xyz_range(cubes_start, cubes_stop, step=step):
            # NOTE Can fan-out these calls
            try:
                downsample_volume(args, target, step, dim, use_iso_flag)
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise

    # Resize the coordinate frame extents as well
    def resize(var, size):
        args[var + '_start'] //= size
        args[var + '_stop'] = ceildiv(args[var + '_stop'], size)
    resize('x', 2)
    resize('y', 2)
    #resize('z', 1)

    # Advance the loop and recalculate the conditional
    args['resolution'] = resolution + 1
    args['res_lt_max'] = args['resolution'] < args['resolution_max']
    return args

def downsample_volume(args, target, step, dim, use_iso_key):
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
        log.debug("Downloading cube {}".format(cube))

        try:
            obj_key = HashedKey(parent_iso, col_id, exp_id, chan_id, resolution, t, cube.morton, version=version)
            data = s3.get(obj_key)
            data = blosc.decompress(data)

            # DP ???: Check to see if the buffer is all zeros?
            data = Buffer.frombuffer(data, dtype=np_types[data_type]).resize(dim)

            volume[offset * dim: (offset + 1) * dim] = data
            volume_empty = False
        except Exception:
            log.debug("No cube at {}".format(cube))

    if volume_empty:
        log.debug("Completely empty volume, not downsampling")
        return

    # Create downsampled cube
    new_dim = XYZ(*CUBOIDSIZE[resolution + 1])
    cube =  Buffer.zeros(new_dim, dtype=np_types[data_type], order='C')
    cube.dim = new_dim
    cube.cubes = XYZ(1,1,1)

    downsample_cube(volume, cube, annotation_chan)

    # Compress the new cube before saving
    cube = blosc.compress(cube, typesize=(np.dtype(cube.dtype).itemsize))

    # Save new cube in S3
    obj_key = HashedKey(iso, col_id, exp_id, chan_id, resolution + 1, t, target.morton, version=version)
    s3.put(obj_key, cube)

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

    # Update ID Index if the channel is an annotation channel
    if annotation_chan:
        ids = ndlib.unique(cube)

        # Convert IDs to strings and drop any IDs that equal zero
        ids = [str(id) for id in ids if id != 0]

        if len(ids) > 0:
            idx_key = S3IndexKey(obj_key, version)
            s3_index.update_ids(idx_key, id_strs)

            for id in ids:
                idx_key = HashedKey(iso, col_id, exp_id, chan_id, resolution + 1, id)
                chan_key = IdIndexKey(idx_key, version)
                id_index.update_id(chan_key, obj_key)

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
    log.debug("downsample_cube({}, {}, {})".format(volume.shape, cube.shape, is_annotation))

    def most_occurrences(xs):
        counts = {}
        for x in xs:
            if x not in counts:
                counts[x] = xs.count(x)

        return max(counts, key=lambda x: counts[x])

    if is_annotation:
        # Foreach output 'pixel', select the source annotation that appears the most
        # right now code assumes cur_dim == dim, if not need to pass a scale value
        for xyz in xyz_range(cube.dim):
            start = xyz * volume.cubes # scale up from output cube to input volume
            stop = start + volume.cubes # iterate over volume.cubes worth of 'pixels'

            annotations = [volume[idx] for idx in xyz_range(start, stop)]

            cube[xyz] = most_occurrences(annotations)
    else:
        # Foreach output z slice, use Image to shrink the input slize(s)
        for z in range(cube.dim.z):
            if volume.cubes.z == 2:
                # Take two Z slices and merge them together
                slice1 = volume[z * 2, :, :]
                slice2 = volume[z * 2 + 1, :, :]
                slice = isotropicBuild_ctype(slice1, slice2)
            else:
                slice = volume[z, :, :]

            if volume.dtype == np.uint8:
                image_type = 'L'
            elif volume.dtype == np.uint16:
                image_type = 'I;16'
            else:
                raise Exception("Unsupported type for image downsampling '{}'".format(volume.dtype))

            image = Image.frombuffer(image_type,
                                     (volume.shape.x, volume.shape.y),
                                     slice.flatten(),
                                     'raw',
                                     image_type,
                                     0, 1)

            cube[z, :, :] = Buffer.asarray(image.resize((cube.shape.x, cube.shape.y)))
