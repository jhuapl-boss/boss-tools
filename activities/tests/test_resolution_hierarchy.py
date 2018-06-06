# Copyright 2018 The Johns Hopkins University Applied Physics Laboratory
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

# lambdafcns contains symbolic links to lambda functions in boss-tools/lambda.
# Since lambda is a reserved word, this allows importing from that folder 
# without updating scripts responsible for deploying the lambda code.

import os
import sys
import importlib
import json
import types
import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta

import boto3
from moto import mock_sqs, mock_dynamodb, mock_lambda

#import logging.config
#logging.config.dictConfig({
#    'version': 1,
#    'disable_existing_loggers': False,
#    'formatters': { 'detailed': { 'format': '%(message)s' } },
#    'handlers': { 'console': { 'class': 'logging.StreamHandler',
#                               'formatter': 'detailed' } },
#    'root': { 'handlers': ['console'],
#              'level': 'DEBUG' }
#})

####################################################
################## CUSTOM MOCKING ##################
ACCOUNT_ID = '123456789012'
REGION = 'us-east-1'
SESSION = boto3.session.Session(region_name = REGION)
SQS_URL = 'http://sqs.{}.amazonaws.com/{}/'.format(REGION, ACCOUNT_ID)

# Stub XYZMorton implementation that returns a unique number when called
def XYZMorton(xyz):
    x,y,z = xyz
    morton = (x << 40) | (y << 20) | (z)
    return morton

# Stub ndtype.CUBOIDSIZE
class CUBOIDSIZE(object):
    def __getitem__(self, key):
        return (512, 512, 16)

class DateTime(datetime):
    @staticmethod
    def now():
        return datetime(year=1970, month=1, day=1)

class MultiprocessingPool(object):
    def __init__(self, size):
        self.size = size

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        return False

    def starmap(self, func, args):
        rtn = []
        for arg in args:
            rtn.append(func(*arg))
        return rtn

def make_check_queue(**kwargs):
    """kwargs:
           arn: values
    """
    def wrap(values):
        if type(values) == list:
            for value in values:
                yield value
            while True:
                yield 0
        else:
            while True:
                yield values
    arns = {
        arn: wrap(kwargs[arn])
        for arn in kwargs
    }

    def check_queue(arn):
        return next(arns[arn])

    return check_queue

# Stub AWS Lambda Client with invoke and exceptions
class MockLambda(object):
    exceptions_ = {
        'RequestTooLargeException': type('RequestTooLargeException', (Exception, ), {}),
        'TooManyRequestsException': type('TooManyRequestsException', (Exception, ), {}),
        'EC2ThrottledException': type('EC2ThrottledException', (Exception, ), {}),

        # Stand-in for any other Exception that boto3 may throw
        'UnknownException': type('UnknownException', (Exception, ), {}),
    }

    def __init__(self):
        self.invoke = MagicMock()

    def __getattr__(self, name):
        if name == 'exceptions':
            return self
        elif name in MockLambda.exceptions_:
            return MockLambda.exceptions_[name]
        else:
            return AttributeError()

# Mockup import for resolution_hierarchy and bossutils.multidimensional
sys.modules['bossutils'] = MagicMock()
sys.modules['bossutils'].aws.get_session.return_value = SESSION
sys.modules['numpy'] = MagicMock()
sys.modules['spdb.c_lib'] = MagicMock()
sys.modules['spdb.c_lib'].ndlib.XYZMorton = XYZMorton
sys.modules['spdb.c_lib.ndtype'] = MagicMock()
sys.modules['spdb.c_lib.ndtype'].CUBOIDSIZE = CUBOIDSIZE()

# Import the current verions of bossutils.multidimensional so that all of the
# 3D math works correctly
cur_dir = os.path.dirname(os.path.realpath(__file__))
bossutils_dir = os.path.normpath(os.path.join(cur_dir, '..', '..', 'bossutils'))
sys.path.insert(0, bossutils_dir)
md = importlib.import_module('multidimensional')
sys.modules['bossutils.multidimensional'] = md
sys.path.pop(0)

################## END CUSTOM MOCKING ##################
########################################################

# Import the code to be tested 
import resolution_hierarchy as rh
rh.POOL_SIZE = 2
rh.MAX_LAMBDA_TIME = rh.timedelta()
rh.datetime = DateTime
rh.Pool = MultiprocessingPool


"""
Test isotropic normal
Test anisotropic normal
Test anisotropic at the isotropic split
Test anisotropic after the isotropic split
Test non even frame size (frame size is not an even multiple of cube size)
Test non zero frame start
Test resolution / resolution_max logic so it doesn't change

Test passing in downsample_id
Always create / delete the queue or create it when creating the downsample_id and delete when res_lt_max is False?
"""

@patch.object(rh, 'delete_queue')
@patch.object(rh, 'launch_lambdas')
@patch.object(rh, 'populate_cubes', return_value = 1)
@patch.object(rh, 'create_queue', side_effect = lambda name: SQS_URL + name)
@patch.object(rh.random, 'random', return_value = 0.1234)
class TestDownsampleChannel(unittest.TestCase):
    def get_args(self, scale=2, **kwargs):
        cube_size = md.XYZ(*CUBOIDSIZE()[0])
        frame_stop = cube_size * scale
        args = {
            # Only including keys that the Activity uses, the others are passed
            # to the downsample_volume lambda without inspection
            'downsample_volume_lambda': 'lambda-arn',

            'x_start': 0,
            'y_start': 0,
            'z_start': 0,

            'x_stop': int(frame_stop.x), # int() to handle float scale multiplication results
            'y_stop': int(frame_stop.y),
            'z_stop': int(frame_stop.z),

            'resolution': 0,
            'resolution_max': 3,
            'res_lt_max': True,

            'type': 'isotropic',
            'iso_resolution': 3,
        }
        args.update(kwargs)

        return args

    def test_downsample_channel_iso(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='isotropic')
        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = [call('downsample-dlq-1234'),
                    call('downsample-cubes-1234')]
        self.assertEqual(mCreateQueue.mock_calls, expected)

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,2))
        self.assertEqual(mPopulateCubes.mock_calls, [expected])

        args = {
            'bucket_size': rh.BUCKET_SIZE,
            'args': self.get_args(type='isotropic'), # Need the original arguments
            'step': md.XYZ(2,2,2),
            'dim': md.XYZ(512, 512, 16),
            'use_iso_flag': False,
            'dlq_arn': SQS_URL + 'downsample-dlq-1234',
            'cubes_arn': SQS_URL + 'downsample-cubes-1234'
        }
        expected = call(mPopulateCubes.return_value,
                        args1['downsample_volume_lambda'],
                        json.dumps(args).encode('UTF8'),
                        SQS_URL + 'downsample-dlq-1234',
                        SQS_URL + 'downsample-cubes-1234')
        self.assertEqual(mLaunchLambdas.mock_calls, [expected])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 16)

        self.assertEqual(args2['resolution'], 1)
        self.assertTrue(args2['res_lt_max'])

        expected = [call(SQS_URL + 'downsample-dlq-1234'),
                    call(SQS_URL + 'downsample-cubes-1234')]
        self.assertEqual(mDeleteQueue.mock_calls, expected)

    def test_downsample_channel_aniso(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='anisotropic')
        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = [call('downsample-dlq-1234'),
                    call('downsample-cubes-1234')]
        self.assertEqual(mCreateQueue.mock_calls, expected)

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,1))
        self.assertEqual(mPopulateCubes.mock_calls, [expected])

        args = {
            'bucket_size': rh.BUCKET_SIZE,
            'args': self.get_args(type='anisotropic'), # Need the original arguments
            'step': md.XYZ(2,2,1),
            'dim': md.XYZ(512, 512, 16),
            'use_iso_flag': False,
            'dlq_arn': SQS_URL + 'downsample-dlq-1234',
            'cubes_arn': SQS_URL + 'downsample-cubes-1234'
        }
        expected = call(mPopulateCubes.return_value,
                        args1['downsample_volume_lambda'],
                        json.dumps(args).encode('UTF8'),
                        SQS_URL + 'downsample-dlq-1234',
                        SQS_URL + 'downsample-cubes-1234')
        self.assertEqual(mLaunchLambdas.mock_calls, [expected])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 32)

        self.assertNotIn('iso_x_start', args2)

        self.assertEqual(args2['resolution'], 1)
        self.assertTrue(args2['res_lt_max'])

        expected = [call(SQS_URL + 'downsample-dlq-1234'),
                    call(SQS_URL + 'downsample-cubes-1234')]
        self.assertEqual(mDeleteQueue.mock_calls, expected)

    def test_downsample_channel_aniso_split(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='anisotropic', iso_resolution=1)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertIn('iso_x_start', args2)

        self.assertEqual(args2['iso_x_stop'], 512)
        self.assertEqual(args2['iso_y_stop'], 512)
        self.assertEqual(args2['iso_z_stop'], 32)

    def test_downsample_channel_aniso_post_split(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='anisotropic',
                              iso_resolution=0,
                              iso_x_start = 0, iso_y_start = 0, iso_z_start = 0,
                              iso_x_stop = 1024, iso_y_stop = 1024, iso_z_stop = 32)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,1))
        expected1 = call(SQS_URL + 'downsample-cubes-1234',
                         md.XYZ(0,0,0),
                         md.XYZ(2,2,2),
                         md.XYZ(2,2,2))
        self.assertEqual(mPopulateCubes.mock_calls, [expected, expected1])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 32)

        self.assertEqual(args2['iso_x_stop'], 512)
        self.assertEqual(args2['iso_y_stop'], 512)
        self.assertEqual(args2['iso_z_stop'], 16)

    def test_downsample_channel_not_even(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='isotropic', scale=2.5)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        md.XYZ(0,0,0),
                        md.XYZ(3,3,3), # Scaled up from 2.5 to 3 cubes that will be downsampled
                        md.XYZ(2,2,2))
        self.assertEqual(mPopulateCubes.mock_calls, [expected])

        self.assertEqual(args2['x_stop'], 640) # 640 = 512 * 2.5 / 2 (scaled up volume and then downsampled)
        self.assertEqual(args2['y_stop'], 640)
        self.assertEqual(args2['z_stop'], 20) # 20 = 16 * 2.5 / 2

    def test_downsample_channel_non_zero_start(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        ###### NOTES ######
        # Derek: I believe that downsampling a non-zero start frame actually downsamples the data correctly
        #        the problem that we run into is that the frame start shrinks towards zero. This presents an
        #        issue when the Channel's Frame starts at the initial offset and downsampling shrinks the frame
        #        for lower resolutions beyond the lower extent of the Channel's Frame, makng that data unavailable.
        args1 = self.get_args(type='isotropic', scale=3,
                              x_start=512, y_start=512, z_start=16) # Really two cubes offset by one cube

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        md.XYZ(1,1,1),
                        md.XYZ(3,3,3),
                        md.XYZ(2,2,2))
        self.assertEqual(mPopulateCubes.mock_calls, [expected])

        self.assertEqual(args2['x_start'], 256) # 512 / 2
        self.assertEqual(args2['y_start'], 256)
        self.assertEqual(args2['z_start'], 8) # 16 / 2

        self.assertEqual(args2['x_stop'], 768) # 512 * 3 / 2
        self.assertEqual(args2['y_stop'], 768)
        self.assertEqual(args2['z_stop'], 24) # 16 * 3 / 2

    def test_downsample_channel_before_stop(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='isotropic',
                              resolution = 0, resolution_max = 3)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['resolution'], 1)
        self.assertTrue(args2['res_lt_max'])

    def test_downsample_channel_at_stop(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        args1 = self.get_args(type='isotropic',
                              resolution = 1, resolution_max = 3)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['resolution'], 2)
        self.assertFalse(args2['res_lt_max'])

    def test_downsample_channel_after_stop(self, mRandom, mCreateQueue, mPopulateCubes, mLaunchLambdas, mDeleteQueue):
        ###### NOTES ######
        # Derek: Not a real world call, as the input arguments are no longer valid
        args1 = self.get_args(type='isotropic',
                              resolution = 2, resolution_max = 3)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['resolution'], 3)
        self.assertFalse(args2['res_lt_max'])

class TestChunk(unittest.TestCase):
    def test_exact(self):
        args = [1,2,3,4]

        gen = rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = [[1,2], [3,4]]

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks, expected)

    def test_not_exist(self):
        args = (i for i in [1,2,3,4,5])

        gen = rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = [[1,2], [3,4], [5]]

        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks, expected)

    def test_empty(self):
        args = []

        gen = rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = []

        self.assertEqual(len(chunks), 0)
        self.assertEqual(chunks, expected)

class TestCubes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.start = md.XYZ(0,0,0)
        cls.stop = md.XYZ(2,2,2)
        cls.step = md.XYZ(1,1,1)

    def test_num_cubes(self):
        num = rh.num_cubes(self.start, self.stop, self.step)
        self.assertEqual(num, 8)

    def test_make_cubes(self):
        gen = rh.make_cubes(self.start, self.stop, self.step)

        self.assertEqual(type(gen), types.GeneratorType)

        cubes = list(gen)
        expected = [md.XYZ(0,0,0),
                    md.XYZ(0,0,1),
                    md.XYZ(0,1,0),
                    md.XYZ(0,1,1),
                    md.XYZ(1,0,0),
                    md.XYZ(1,0,1),
                    md.XYZ(1,1,0),
                    md.XYZ(1,1,1)]

        self.assertEqual(len(cubes), 8)
        self.assertEqual(cubes, expected)

class TestEnqueue(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.name = 'downsample-1234'
        cls.arn = SQS_URL + cls.name
        cls.start = md.XYZ(0,0,0)
        cls.stop = md.XYZ(2,2,2)
        cls.step = md.XYZ(1,1,1)
        cls.chunk = [md.XYZ(0,0,0),
                     md.XYZ(1,1,1)]

    @patch.object(rh, 'enqueue_cubes')
    def test_populate(self, mEnqueueCubes):
        count = rh.populate_cubes(self.arn, self.start, self.stop, self.step)

        self.assertEqual(count, 8)

        expected = [call(self.arn, [md.XYZ(0,0,0),
                                    md.XYZ(0,0,1),
                                    md.XYZ(0,1,0),
                                    md.XYZ(0,1,1)]),
                    call(self.arn, [md.XYZ(1,0,0),
                                    md.XYZ(1,0,1),
                                    md.XYZ(1,1,0),
                                    md.XYZ(1,1,1)])]
        self.assertEqual(mEnqueueCubes.mock_calls, expected)

    @patch.object(SESSION, 'resource')
    def test_enqueue(self, mResource):
        rh.enqueue_cubes(self.arn, self.chunk)

        func = mResource.return_value.Queue.return_value.send_messages
        expected = [call(Entries=[{'Id': str(id(self.chunk[0])),
                                   'MessageBody': '[0, 0, 0]'},
                                  {'Id': str(id(self.chunk[1])),
                                   'MessageBody': '[1, 1, 1]'}])]
        self.assertEqual(func.mock_calls, expected)

    @patch.object(SESSION, 'resource')
    def test_enqueue_multi_batch(self, mResource):
        chunk = range(15)
        rh.enqueue_cubes(self.arn, chunk)

        func = mResource.return_value.Queue.return_value.send_messages
        self.assertEqual(len(func.mock_calls), 2)

        # index 2 of a call is the kwargs for the call
        self.assertEqual(len(func.mock_calls[0][2]['Entries']), 10)
        self.assertEqual(len(func.mock_calls[1][2]['Entries']), 5)

    @patch.object(SESSION, 'resource')
    def test_enqueue_error(self, mResource):
        func = mResource.return_value.Queue.return_value.send_messages
        func.side_effect = Exception("Could not enqueue data")

        with self.assertRaises(rh.ResolutionHierarchyError):
            rh.enqueue_cubes(self.arn, self.chunk)

class TestInvoke(unittest.TestCase):
    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue', side_effect = make_check_queue(dlq_arn = 0, cubes_arn = [5,4,3,2,1]))
    @patch.object(rh, 'invoke_lambdas')
    def test_launch(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE)
        self.assertEqual(len(mCheckQueue.mock_calls), (5 + rh.ZERO_COUNT) * 2) # (5+) for cubes_arn, (*2) for both queues

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue', side_effect = make_check_queue(dlq_arn = 0, cubes_arn = 0))
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_empty(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE)
        self.assertEqual(len(mCheckQueue.mock_calls), rh.ZERO_COUNT * 2) # * 2, one for each queue

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue', side_effect = make_check_queue(dlq_arn = 1, cubes_arn = 0))
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_dlq(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        count = 10

        with self.assertRaises(rh.FailedLambdaError):
            rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE)
        self.assertEqual(len(mCheckQueue.mock_calls), 1) # single dlq check_queue call

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue')
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_relaunch(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        cubes_count = [5] * rh.UNCHANGING_LAUNCH + [4,3,2,1]
        mCheckQueue.side_effect = make_check_queue(dlq_arn = 0, cubes_arn = cubes_count)

        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mCheckQueue.mock_calls), (len(cubes_count) + rh.ZERO_COUNT) * 2) # (*2) for both queues

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue')
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_relaunch_multi(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        cubes_count = [5] * rh.UNCHANGING_LAUNCH + [4] * rh.UNCHANGING_LAUNCH + [3,2,1]
        mCheckQueue.side_effect = make_check_queue(dlq_arn = 0, cubes_arn = cubes_count)

        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE + 2) # +2 relaunches
        self.assertEqual(len(mCheckQueue.mock_calls), (len(cubes_count) + rh.ZERO_COUNT) * 2) # (*2) for both queues

    @patch.object(rh, 'lambda_throttle_count')
    @patch.object(rh, 'check_queue')
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_throttle(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        cubes_count = [5] * rh.UNCHANGING_THROTTLE + [4,3,2,1]
        throttle_count = [2] * rh.UNCHANGING_THROTTLE + [2,2,2,1,0,0,0,0,0,0,0,0,0]
        mCheckQueue.side_effect = make_check_queue(dlq_arn = 0, cubes_arn = cubes_count)
        mLambdaThrottleCount.side_effect = throttle_count

        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mCheckQueue.mock_calls), ((len(cubes_count) + rh.ZERO_COUNT) * 2) + 2) # (+2) for 2 extra dlq polls in throttling loop, (*2) for both queues

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue')
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_no_throttle(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        cubes_count = [5] * rh.UNCHANGING_THROTTLE + [4,3,2,1]
        mCheckQueue.side_effect = make_check_queue(dlq_arn = 0, cubes_arn = cubes_count)

        count = 10
        rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mCheckQueue.mock_calls), (len(cubes_count) + rh.ZERO_COUNT) * 2) # (*2) for both queues

    @patch.object(rh, 'lambda_throttle_count', return_value = 0)
    @patch.object(rh, 'check_queue')
    @patch.object(rh, 'invoke_lambdas')
    def test_launch_unchanging_max(self, mInvokeLambdas, mCheckQueue, mLambdaThrottleCount):
        cubes_count = [5] * rh.UNCHANGING_MAX
        mCheckQueue.side_effect = make_check_queue(dlq_arn = 0, cubes_arn = cubes_count)

        with self.assertRaises(rh.ResolutionHierarchyError):
            count = 10
            rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn')

        self.assertEqual(len(mInvokeLambdas.mock_calls), rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mCheckQueue.mock_calls), len(cubes_count) * 2) # (*2) for both queues

    @patch.object(SESSION, 'client')
    def test_invoke(self, mClient):
        invoke = mClient.return_value.invoke

        count = 2
        rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

        expected = call(FunctionName = 'lambda_arn',
                        InvocationType = 'Event',
                        Payload = '{}')

        self.assertEqual(len(invoke.mock_calls), count)
        self.assertEqual(invoke.mock_calls[0], expected)

    @patch.object(SESSION, 'client')
    def test_invoke_error(self, mClient):
        invoke = mClient.return_value.invoke
        invoke.side_effect = Exception("Could not invoke lambda")

        with self.assertRaises(rh.ResolutionHierarchyError):
            count = 2
            rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

    @patch.object(rh, 'check_queue', return_value = 1)
    @patch.object(SESSION, 'client')
    def test_invoke_dlq(self, mClient, mCheckQueue):
        invoke = mClient.return_value.invoke

        with self.assertRaises(rh.ResolutionHierarchyError):
            count = 12
            rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

        self.assertEqual(len(invoke.mock_calls), 9)

class TestQueue(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.downsample_id = '1234'
        cls.name = 'downsample-{}'.format(cls.downsample_id)
        cls.url = SQS_URL + cls.name

    def assertSuccess(self):
        self.assertTrue(True)

    @mock_sqs
    def test_create_queue(self):
        actual = rh.create_queue(self.name)

        self.assertEqual(self.url, actual)

    @mock_sqs
    def test_create_queue_already_exist(self):
        # SQS create_queue will not error if the same queue tries to be created

        first = rh.create_queue(self.downsample_id)
        second = rh.create_queue(self.downsample_id)

        self.assertEqual(first, second)

    @mock_sqs
    def test_delete_queue(self):
        SESSION.client('sqs').create_queue(QueueName = self.name)

        rh.delete_queue(self.url)

        self.assertSuccess()

    @mock_sqs
    def test_delete_queue_doesnt_exist(self):
        # delete_queue logs the error
        rh.delete_queue(self.url)

        self.assertSuccess()

    @mock_sqs
    def test_check_queue_doesnt_exist(self):
        count = rh.check_queue(self.url)

        self.assertEqual(count, 0)

    @mock_sqs
    def test_check_queue_empty(self):
        arn = rh.create_queue(self.downsample_id)

        count = rh.check_queue(arn)

        self.assertEqual(count, 0)

    @mock_sqs
    def test_check_queue_non_empty(self):
        arn = rh.create_queue(self.downsample_id)
        SESSION.client('sqs').send_message(QueueUrl = arn,
                                           MessageBody = 'message')
        SESSION.client('sqs').send_message(QueueUrl = arn,
                                           MessageBody = 'message')

        count = rh.check_queue(arn)

        self.assertEqual(count, 2)

@patch.object(SESSION, 'client')
class TestThrottle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.statistics = {
            'Datapoints': [{
                'SampleCount': 123
            }]
        }

    def mock_client(self, client, return_data=True):
        gms = client.return_value.get_metric_statistics
        if return_data is True:
            gms.return_value = self.statistics
        elif return_data is False:
            gms.return_value = {}
        elif return_data is None:
            gms.side_effect = Exception("Could not get metrics")

    def test_throttle_metric(self, mClient):
        self.mock_client(mClient, return_data=True)

        arn = "arn:aws:lambda:region:account:function:name"
        count = rh.lambda_throttle_count(arn)

        self.assertEqual(count, 123)

        actual = mClient.return_value.get_metric_statistics.mock_calls
        expected = [call(Namespace = 'AWS/Lambda',
                         MetricName = 'Throttles',
                         Dimensions = [{'Name': 'FunctionName', 'Value': 'name'}],
                         StartTime = DateTime.now() - timedelta(minutes=1),
                         EndTime = DateTime.now(),
                         Period = 60,
                         Unit = 'Count',
                         Statistics = ['SampleCount'])]
        self.assertEqual(actual, expected)

    def test_throttle_no_metric(self, mClient):
        self.mock_client(mClient, return_data=False)

        count = rh.lambda_throttle_count('lambda_arn')

        self.assertEqual(count, 0)

    def test_throttle_error(self, mClient):
        self.mock_client(mClient, return_data=None)

        count = rh.lambda_throttle_count('lambda_arn')

        self.assertEqual(count, -1)

