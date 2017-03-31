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

import numpy as np
from collections import namedtuple
from spdb.c_lib import ndlib

# Do an inverted floordiv, works for python bigints
ceildiv = lambda a, b: -(-a // b)

def isvector(v):
    return isinstance(v, (XYZ, ZYX))

def extract_xyz(v):
    if isvector(v):
        return v.x, v.y, v.z
    else:
        return v, v, v

class VectorMathMixin(object):
    @property
    def morton(self):
        return ndlib.XYZMorton((self.x, self.y, self.z))

    @classmethod
    def from_morton(cls, morton):
        x,y,z = ndlib.MortonXYZ(morton)
        return cls(x=x, y=y, z=z)

    @property
    def xyz(self):
        if isinstance(self, XYZ):
            return self
        else:
            return XYZ(self.x, self.y, self.z)

    @property
    def zyx(self):
        if isinstance(self, ZYX):
            return self
        else:
            return ZYX(self.z, self.y, self.x)

    def __add__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x + x,
                          y = self.y + y,
                          z = self.z + z)

    def __sub__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x - x,
                          y = self.y - y,
                          z = self.z - z)

    def __mul__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x * x,
                          y = self.y * y,
                          z = self.z * z)

    def __truediv__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x / x,
                          y = self.y / y,
                          z = self.z / z)

    def __floordiv__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x // x,
                          y = self.y // y,
                          z = self.z // z)

    def __mod__(self, other):
        x,y,z = extract_xyz(other)
        return type(self)(x = self.x % x,
                          y = self.y % y,
                          z = self.z % z)

    def __neg__(self):
        return type(self)(x = -self.x,
                          y = -self.y,
                          z = -self.z)

# GENERIC 3D utilities
# DP ???: Create XYZT?
class XYZ(VectorMathMixin, namedtuple('XYZ', ['x', 'y', 'z'])):
    __slots__ = ()

class ZYX(VectorMathMixin, namedtuple('ZYX', ['z', 'y', 'x'])):
    __slots__ = ()

range_ = range
def range(*args, step=None):
    if len(args) == 2:
        start, stop = args
    else:
        stop, = args
        start = type(stop)(0,0,0)
    cls = type(start)

    if step is None:
        step = cls(1,1,1)

    # DP ???: iterate in the order of the arguments?
    # (would have to make sure start/stop are same type)
    # DP NOTE: need int() to handle divsion with XYZ types
    for x in range_(int(start.x), int(stop.x), int(step.x)):
        for y in range_(int(start.y), int(stop.y), int(step.y)):
            for z in range_(int(start.z), int(stop.z), int(step.z)):
                yield cls(x=x, y=y, z=z)

slice_ = slice
def slice(args):
    start = stop = step = (None, None, None)
    if len(args) == 3:
        start, stop, step = args
    if len(args) == 2:
        start, stop = args
    else:
        stop, = args

    return (slice_(start[0], stop[0], step[0]),
            slice_(start[1], stop[1], step[1]),
            slice_(start[2], stop[2], step[2]))

def isvectorslice(s):
    NoneType = type(None)
    return isinstance(s, slice_) and \
           isinstance(s.start, (XYZ, ZYX, NoneType)) and \
           isinstance(s.stop, (XYZ, ZYX, NoneType)) and \
           isinstance(s.step, (XYZ, ZYX, NoneType))

def tovectorslice(s):
    def extract(v):
        return (v[0], v[1], v[2]) if isvector(v) else (v, v, v)

    ba, bb, bc = extract_xyz(s.start)
    ea, eb, ec = extract_xyz(s.stop)
    sa, sb, sc = extract_xyz(s.step)

    # slice ZYX
    return (slice_(bc, ec, sc),
            slice_(bb, eb, sb),
            slice_(ba, ea, sa))

class Buffer(np.ndarray):
    def __new__(cls, *args, **kwargs):
        obj = np.ndarray.__new__(cls, *args, **kwargs)
        return obj.view(cls)

    #def __array_finalize__(self, obj):
    #    if obj is None: return
    #    return

    def __getitem__(self, key):
        if isvectorslice(key):
            key = tovectorslice(key)

        return super(Buffer, self).__getitem__(key)

    def __setitem__(self, key, val):
        if isvectorslice(key):
            key = tovectorslice(key)

        return super(Buffer, self).__setitem__(key, val)

    def resize(self, new_shape, **kwargs):
        if isvector(new_shape):
            new_shape = new_shape.zyx

        return super(Buffer, self).resize(new_shape, **wargs)

    # DP TODO: implement setter
    @property
    def shape(self):
        s = super(Buffer, self).shape
        return XYZ(s[2], s[1], s[0]) # convert from ZYX to XYZ

    @classmethod
    def zeros(cls, shape, **kwargs):
        if isvector(shape):
            shape = shape.zyx

        return np.zeros(shape, **kwargs).view(cls)

    @classmethod
    def frombuffer(cls, *args, **kwargs):
        return np.frombuffer(*args, **kwargs).view(cls)

    @classmethod
    def asarray(cls, *args, **kwargs):
        return np.asarray(*args, **kwargs).view(cls)

class XYZVolume(list):
    def __init__(self, xyz, default=b''):
        super().__init__()
        self.xyz = xyz
        for x in range(xyz.x):
            ys = []
            for y in range(xyz.y):
                ys.append([default for z in range(xyz.z)])
            self.append(ys)

    def __setitem__(self, key, value):
        if type(key) == XYZ:
            x, y, z = key
            self[x][y][z] = value
        else:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        if type(key) == XYZ:
            x, y, z = key
            return self[x][y][z]
        else:
            return super().__getitem__(key)

