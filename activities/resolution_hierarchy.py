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
from scipy.ndimage.interpolation import zoom
from collections import namedtuple
from bossutils import aws, logger
from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

from multidimensional import XYZ, Buffer, ceildiv
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
            data_type (str) 'uint8' | 'uint16' | 'uint64'

            s3_bucket (URL)
            s3_index (URL)
            id_index (URL)

            x_start (int)
            y_start (int)
            z_start (int)

            x_stop (int)
            y_stop (int)
            z_stop (int)

            resolution (int) The resolution to downsample. Creates resolution + 1
            resolution_max (int) The maximum resolution to generate
            res_lt_max (bool) = args['resolution'] < (args['resolution_max'] - 1)

            type (str) 'isotropic' | 'anisotropic'
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both
        }
    """

    #log.debug("Downsampling resolution " + str(args['resolution']))

    resolution = args['resolution']

    dim = XYZ(*CUBOIDSIZE[resolution])
    #log.debug("Cube dimensions: {}".format(dim))

    def frame(key):
        return XYZ(args[key.format('x')], args[key.format('y')], args[key.format('z')])

    configs = []
    if args['type'] == 'isotropic':
        configs.append({
            'name': 'isotropic',
            'step': XYZ(2,2,2),
            'iso_flag': False,
            'frame_start_key': '{}_start',
            'frame_stop_key': '{}_stop',
        })
    else:
        configs.append({
            'name': 'anisotropic',
            'step': XYZ(2,2,1),
            'iso_flag': False,
            'frame_start_key': '{}_start',
            'frame_stop_key': '{}_stop',
        })

        if resolution >= args['iso_resolution']: # DP TODO: Figure out how to launch aniso iso version with mutating arguments
            configs.append({
                'name': 'isotropic',
                'step': XYZ(2,2,2),
                'iso_flag': True,
                'frame_start_key': 'iso_{}_start',
                'frame_stop_key': 'iso_{}_stop',
            })

    for config in configs:
        frame_start = frame(config['frame_start_key'])
        frame_stop = frame(config['frame_stop_key'])
        step = config['step']
        use_iso_flag = config['iso_flag']

        cubes_start = frame_start // dim
        cubes_stop = ceildiv(frame_stop, dim)

        log.debug('Downsampling {} resolution {}'.format(config['name'], resolution))
        log.debug("Frame corner: {}".format(frame_start))
        log.debug("Frame extent: {}".format(frame_stop))
        log.debug("Cubes corner: {}".format(cubes_start))
        log.debug("Cubes extent: {}".format(cubes_stop))
        log.debug("Downsample step: {}".format(step))
        for target in xyz_range(cubes_start, cubes_stop, step=step):
            # NOTE Can fan-out these calls
            try:
                downsample_volume(args, target, step, dim, use_iso_flag)
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise

        # Resize the coordinate frame extents as the data shrinks
        def resize(var, size):
            start = config['frame_start_key'].format(var)
            stop = config['frame_stop_key'].format(var)
            args[start] //= size
            args[stop] = ceildiv(args[stop], size)
        resize('x', step.x)
        resize('y', step.y)
        resize('z', step.z)

    # if next iteration will split into aniso and iso downsampling, copy the coordinate frame
    if args['type'] != 'isotropic' and (resolution + 1) == args['iso_resolution']:
        def copy(var):
            args['iso_{}_start'.format(var)] = args['{}_start'.format(var)]
            args['iso_{}_stop'.format(var)] = args['{}_stop'.format(var)]
        copy('x')
        copy('y')
        copy('z')

    # Advance the loop and recalculate the conditional
    # Using max - 1 because resolution_max should not be a valid resolution
    # and res < res_max will end with res = res_max - 1, which generates res_max resolution
    args['resolution'] = resolution + 1
    args['res_lt_max'] = args['resolution'] < (args['resolution_max'] - 1)
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

    # Update ID Index if the channel is an annotation channel
    if annotation_chan:
        ids = ndlib.unique(cube)

        # Convert IDs to strings and drop any IDs that equal zero
        ids = [str(id) for id in ids if id != 0]

        if len(ids) > 0:
            idx_key = S3IndexKey(obj_key, version)
            s3_index.update_ids(idx_key, ids)

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
    #log.debug("downsample_cube({}, {}, {})".format(volume.shape, cube.shape, is_annotation))

    def most_occurrences(xs):
        counts = {}
        for x in xs:
            if x not in counts:
                counts[x] = xs.count(x)

        return max(counts, key=lambda x: counts[x])

    if is_annotation:
        use_c_version = False
        if use_c_version:
            # C Version of the below Python code
            ndlib.addAnnotationData_ctype(volume, cube, volume.cubes, cube.dim)
        else:
            # Foreach output z slice, use Image to shrink the input slize(s)
            for z in range(cube.dim.z):
                merge_z_slice = False
                if merge_z_slice:
                    if volume.cubes.z == 2:
                        # Take two Z slices and merge them together
                        slice1 = volume[z * 2, :, :]
                        slice2 = volume[z * 2 + 1, :, :]
                        slice = ndlib.isotropicBuild_ctype(slice1, slice2)
                    else:
                        slice = volume[z, :, :]
                else:
                    slice = volume[z * volume.cubes.z, :, :]

                cube[z, :, :] = zoom(slice, 0.5, np.uint64, order=0, mode='nearest')
    else:
        # Foreach output z slice, use Image to shrink the input slize(s)
        for z in range(cube.dim.z):
            merge_z_slice = False
            if merge_z_slice:
                if volume.cubes.z == 2:
                    # Take two Z slices and merge them together
                    slice1 = volume[z * 2, :, :]
                    slice2 = volume[z * 2 + 1, :, :]
                    slice = ndlib.isotropicBuild_ctype(slice1, slice2)
                else:
                    slice = volume[z, :, :]
            else:
                slice = volume[z * volume.cubes.z, :, :]

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

            cube[z, :, :] = Buffer.asarray(image.resize((cube.shape.x, cube.shape.y), Image.BILINEAR))
