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

from collections import namedtuple

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

# Do an inverted floordiv, works for python bigints
ceildiv = lambda a, b: -(-a // b)

# GENERIC 3D utilities
# DP ???: Create XYZT?
class XYZ(namedtuple('XYZ', ['x', 'y', 'z'])):
    __slots__ = ()

    @property
    def morton(self):
        return ndlib.XYZMorton(*self)

    def __add__(self, other):
        return XYZ(self.x + other.x,
                   self.y + other.y,
                   self.z + other.z)

    def __sub__(self, other):
        return XYZ(self.x - other.x,
                   self.y - other.y,
                   self.z - other.z)

class XYZVolume(list):
    def __init__(self, xyz):
        super().__init__()
        for x in range(xyz.x):
            ys = []
            for y in range(xyz.y):
                ys.append([]) # Z dimension
            self.append(ys)

    def __getitem__(self, key):
        if type(key) == XYZ:
            x, y, z = key
            return self[x][y][z]
        else:
            return super().__getitem__(key)

def xyz_range(*args, step=None):
    if len(args) == 2:
        start, stop = args
    else:
        stop, = args
        start = XYZ(0,0,0)

    if step is None:
        step = XYZ(1,1,1)

    for x in range(start.x, stop.x, step.x):
        for y in range(start.y, stop.y, step.y):
            for z in range(start.z, stop.z, step.z):
                yield XYZ(x, y, z)

# BOSS Key creation functions / classes
class S3IndexKey(dict):
    def __init__(self, obj_key, version=0):
        super().__init__()
        self['object-key'] = {'S': obj_key},
        self['version-node'] = {'N': str(version)}

def S3ObjectKey(*args, version=0):
    """
    Args:
        collection_id
        experiment_id
        channel_id
        resolution
        time_sample
        morton (str): Morton ID of cube
        version : Optional Object version
    """
    key = '&'.join(map(str, args))
    digest = hashlib.md5(key.encode()).hexdigest()
    return '{}&{}&{}'.format(digest, key, version)

# ACTUAL Activities
def foo(frame, resolution):
    dynamodb = boto3.client('dynamodb')
    s3 = boto3.client('s3')

    col_id = 0
    exp_id = 0
    chan_id = 0
    version = 0
    t = 0
    s3_bucket = ''

    # Assume starting from 0,0,0
    dim = XYZ(*CUBOIDSIZE[resolution])

    cubes = XYZ(ceildiv(frame.x_stop, dim.x),
                ceildiv(frame.y_stop, dim.y),
                ceildiv(frame.z_stop, dim.z))

    cube_data = {}
    for xyz in xyz_range(cubes):
        obj_key = S3ObjectKey(col_id, exp_id, chan_id, resolution, t, xyz.morton)

        resp = s3.get_object(Key = obj_key,
                             Bucket = s3_bucket)

        # TODO check for error

        obj_data = resp['Body'].read()

        cube_data[xyz] = obj_data

    one = XYZ(1,1,1)
    step = XYZ(2,2,1)
    for target in xyz_range(cubes, step=step):
        volume = XYZVolume(step)
        for cube in xyz_range(target, target + step):
            volume[cube - target] = cube_data[xyz]

        cube = downsample(volume)

        obj_key = S3ObjectKey(col_id, exp_id, chan_id, resolution + 1, t, target.morton)
        resp = s3.put_object(Key = obj_key,
                             Body = cube,
                             Bucket = s3_bucket)

        # TODO check for error

        # Update indicies


def downsample(volume):
    raise NotImplemented()

