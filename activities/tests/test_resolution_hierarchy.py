# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

"""
Note that the module under test (resolution_hierarchy.py) is imported inside
of the various test classes.  This must be done to ensure that none of the
boto3 clients are instantiated before the moto mocks are put in place.  Also,
environment variables for the AWS keys should be set to a bogus value such as
`testing` to ensure that no real AWS resources are accidentally used.

https://github.com/spulec/moto#how-do-i-avoid-tests-from-mutating-my-real-infrastructure
"""

import sys
import json
import types
import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
from bossutils.multidimensional import ceildiv, XYZ
from ._test_case_with_patch_object import TestCaseWithPatchObject

import boto3
from moto import mock_sqs

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

# This is a fake id as another safe guard against utilizing real AWS resources.
ACCOUNT_ID = '123456789012'
REGION = 'us-east-1'
SESSION = boto3.session.Session(region_name = REGION)
#SQS_URL = 'https://sqs.{}.amazonaws.com/{}/'.format(REGION, ACCOUNT_ID)
SQS_URL = 'https://queue.amazonaws.com/{}/'.format(ACCOUNT_ID)
DOWNSAMPLE_QUEUE_URL = SQS_URL + 'downsample_queue'
RECEIPT_HANDLE = '987654321'

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

# Mockup import for resolution_hierarchy
sys.modules['bossutils'] = MagicMock()
sys.modules['bossutils'].aws.get_session.return_value = SESSION
sys.modules['numpy'] = MagicMock()
sys.modules['spdb.c_lib'] = MagicMock()
sys.modules['spdb.c_lib'].ndlib.XYZMorton = XYZMorton
sys.modules['spdb.c_lib.ndtype'] = MagicMock()
sys.modules['spdb.c_lib.ndtype'].CUBOIDSIZE = CUBOIDSIZE()


################## END CUSTOM MOCKING ##################
########################################################


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

class TestDownsampleChannel(TestCaseWithPatchObject):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

    def setUp(self):
        self.mock_del_queue = self.patch_object(self.rh, 'delete_queue')
        self.mock_launch_lambdas = self.patch_object(self.rh, 'launch_lambdas')
        self.mock_populate_cubes = self.patch_object(self.rh, 'populate_cubes', return_value=1)
        self.mock_create_queue = self.patch_object(self.rh, 'create_queue', side_effect=lambda name: SQS_URL + name)
        mock_rand = self.patch_object(self.rh.random, 'random', return_value=0.1234)

    def get_args(self, scale=2, **kwargs):
        """Create args dict for input to the step function.

        Args:
            scale (int):
            kwargs (dict): Keywords replace key-values in the 'msg' nested dict

        Returns:
            (dict)
        """
        cube_size = XYZ(*CUBOIDSIZE()[0])
        frame_stop = cube_size * scale
        args = {
            # Only including keys that the Activity uses, the others are passed
            # to the downsample_volume lambda without inspection
            'msg': {
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

                'lookup_key': 'fake_lookup_key',
            },

            'queue_url': DOWNSAMPLE_QUEUE_URL,
            'job_receipt_handle': RECEIPT_HANDLE,
        }
        args['msg'].update(kwargs)

        return args

    def test_downsample_channel_iso(self):
        args1 = self.get_args(type='isotropic')
        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = [call('downsample-dlq-1234'),
                    call('downsample-cubes-1234')]
        self.assertEqual(self.mock_create_queue.mock_calls, expected)

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        XYZ(0,0,0),
                        XYZ(2,2,2),
                        XYZ(2,2,2))
        self.assertEqual(self.mock_populate_cubes.mock_calls, [expected])

        args = {
            'bucket_size': self.rh.BUCKET_SIZE,
            'args': self.get_args(type='isotropic')['msg'], # Need the original arguments
            'step': XYZ(2,2,2),
            'dim': XYZ(512, 512, 16),
            'use_iso_flag': False,
            'dlq_arn': SQS_URL + 'downsample-dlq-1234',
            'cubes_arn': SQS_URL + 'downsample-cubes-1234'
        }
        expected = call(ceildiv(self.mock_populate_cubes.return_value, self.rh.BUCKET_SIZE) + self.rh.EXTRA_LAMBDAS,
                        args1['msg']['downsample_volume_lambda'],
                        json.dumps(args).encode('UTF8'),
                        SQS_URL + 'downsample-dlq-1234',
                        SQS_URL + 'downsample-cubes-1234',
                        DOWNSAMPLE_QUEUE_URL,
                        RECEIPT_HANDLE)
        self.assertEqual(self.mock_launch_lambdas.mock_calls, [expected])

        self.assertEqual(args2['msg']['x_stop'], 512)
        self.assertEqual(args2['msg']['y_stop'], 512)
        self.assertEqual(args2['msg']['z_stop'], 16)

        self.assertEqual(args2['msg']['resolution'], 1)
        self.assertTrue(args2['msg']['res_lt_max'])

        expected = [call(SQS_URL + 'downsample-dlq-1234'),
                    call(SQS_URL + 'downsample-cubes-1234')]
        self.assertEqual(self.mock_del_queue.mock_calls, expected)

    def test_downsample_channel_aniso(self):
        args1 = self.get_args(type='anisotropic')
        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = [call('downsample-dlq-1234'),
                    call('downsample-cubes-1234')]
        self.assertEqual(self.mock_create_queue.mock_calls, expected)

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        XYZ(0,0,0),
                        XYZ(2,2,2),
                        XYZ(2,2,1))
        self.assertEqual(self.mock_populate_cubes.mock_calls, [expected])

        args = {
            'bucket_size': self.rh.BUCKET_SIZE,
            'args': self.get_args(type='anisotropic')['msg'], # Need the original arguments
            'step': XYZ(2,2,1),
            'dim': XYZ(512, 512, 16),
            'use_iso_flag': False,
            'dlq_arn': SQS_URL + 'downsample-dlq-1234',
            'cubes_arn': SQS_URL + 'downsample-cubes-1234'
        }
        expected = call(ceildiv(self.mock_populate_cubes.return_value, self.rh.BUCKET_SIZE) + self.rh.EXTRA_LAMBDAS,
                        args1['msg']['downsample_volume_lambda'],
                        json.dumps(args).encode('UTF8'),
                        SQS_URL + 'downsample-dlq-1234',
                        SQS_URL + 'downsample-cubes-1234',
                        DOWNSAMPLE_QUEUE_URL,
                        RECEIPT_HANDLE)
        self.assertEqual(self.mock_launch_lambdas.mock_calls, [expected])

        self.assertEqual(args2['msg']['x_stop'], 512)
        self.assertEqual(args2['msg']['y_stop'], 512)
        self.assertEqual(args2['msg']['z_stop'], 32)

        self.assertNotIn('iso_x_start', args2['msg'])

        self.assertEqual(args2['msg']['resolution'], 1)
        self.assertTrue(args2['msg']['res_lt_max'])

        expected = [call(SQS_URL + 'downsample-dlq-1234'),
                    call(SQS_URL + 'downsample-cubes-1234')]
        self.assertEqual(self.mock_del_queue.mock_calls, expected)

    def test_downsample_channel_aniso_split(self):
        args1 = self.get_args(type='anisotropic', iso_resolution=1, resolution=1)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertIn('iso_x_start', args2['msg'])

        self.assertEqual(args2['msg']['iso_x_stop'], 512)
        self.assertEqual(args2['msg']['iso_y_stop'], 512)
        self.assertEqual(args2['msg']['iso_z_stop'], 16)

    def test_downsample_channel_aniso_post_split(self):
        args1 = self.get_args(type='anisotropic',
                              iso_resolution=0,
                              iso_x_start = 0, iso_y_start = 0, iso_z_start = 0,
                              iso_x_stop = 1024, iso_y_stop = 1024, iso_z_stop = 32)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        XYZ(0,0,0),
                        XYZ(2,2,2),
                        XYZ(2,2,1))
        expected1 = call(SQS_URL + 'downsample-cubes-1234',
                         XYZ(0,0,0),
                         XYZ(2,2,2),
                         XYZ(2,2,2))
        self.assertEqual(self.mock_populate_cubes.mock_calls, [expected, expected1])

        self.assertEqual(args2['msg']['x_stop'], 512)
        self.assertEqual(args2['msg']['y_stop'], 512)
        self.assertEqual(args2['msg']['z_stop'], 32)

        self.assertEqual(args2['msg']['iso_x_stop'], 512)
        self.assertEqual(args2['msg']['iso_y_stop'], 512)
        self.assertEqual(args2['msg']['iso_z_stop'], 16)

    def test_downsample_channel_not_even(self):
        args1 = self.get_args(type='isotropic', scale=2.5)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        XYZ(0,0,0),
                        XYZ(3,3,3), # Scaled up from 2.5 to 3 cubes that will be downsampled
                        XYZ(2,2,2))
        self.assertEqual(self.mock_populate_cubes.mock_calls, [expected])

        self.assertEqual(args2['msg']['x_stop'], 640) # 640 = 512 * 2.5 / 2 (scaled up volume and then downsampled)
        self.assertEqual(args2['msg']['y_stop'], 640)
        self.assertEqual(args2['msg']['z_stop'], 20) # 20 = 16 * 2.5 / 2

    def test_downsample_channel_non_zero_start(self):
        ###### NOTES ######
        # Derek: I believe that downsampling a non-zero start frame actually downsamples the data correctly
        #        the problem that we run into is that the frame start shrinks towards zero. This presents an
        #        issue when the Channel's Frame starts at the initial offset and downsampling shrinks the frame
        #        for lower resolutions beyond the lower extent of the Channel's Frame, makng that data unavailable.
        # Derek: The start / stop values were taken from a non-zero start downsample that showed there was a problem
        #        when the start cube was not even, and therefore didn't align with a full downsample that started
        #        at zero
        args1 = self.get_args(type='isotropic', resolution=4,
                              x_start=4992, y_start=4992, z_start=1232,
                              x_stop=6016,  y_stop=6016,  z_stop=1264)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        expected = call(SQS_URL + 'downsample-cubes-1234',
                        XYZ(8,8,76),
                        XYZ(12,12,79),
                        XYZ(2,2,2))
        self.assertEqual(self.mock_populate_cubes.mock_calls, [expected])

        self.assertEqual(args2['msg']['x_start'], 2496)
        self.assertEqual(args2['msg']['y_start'], 2496)
        self.assertEqual(args2['msg']['z_start'], 616)

        self.assertEqual(args2['msg']['x_stop'], 3008)
        self.assertEqual(args2['msg']['y_stop'], 3008)
        self.assertEqual(args2['msg']['z_stop'], 632)

    def test_downsample_channel_before_stop(self):
        args1 = self.get_args(type='isotropic',
                              resolution = 0, resolution_max = 3)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['msg']['resolution'], 1)
        self.assertTrue(args2['msg']['res_lt_max'])

    def test_downsample_channel_at_stop(self):
        args1 = self.get_args(type='isotropic',
                              resolution = 1, resolution_max = 3)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['msg']['resolution'], 2)
        self.assertFalse(args2['msg']['res_lt_max'])

    def test_downsample_channel_after_stop(self):
        ###### NOTES ######
        # Derek: Not a real world call, as the input arguments are no longer valid
        args1 = self.get_args(type='isotropic',
                              resolution = 2, resolution_max = 3)

        args2 = self.rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['msg']['resolution'], 3)
        self.assertFalse(args2['msg']['res_lt_max'])

class TestChunk(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested 
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

    def test_exact(self):
        args = [1,2,3,4]

        gen = self.rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = [[1,2], [3,4]]

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks, expected)

    def test_not_exist(self):
        args = (i for i in [1,2,3,4,5])

        gen = self.rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = [[1,2], [3,4], [5]]

        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks, expected)

    def test_empty(self):
        args = []

        gen = self.rh.chunk(args, 2) # buckets of size 2

        self.assertEqual(type(gen), types.GeneratorType)

        chunks = list(gen)
        expected = []

        self.assertEqual(len(chunks), 0)
        self.assertEqual(chunks, expected)

class TestCubes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested 
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

        cls.start = XYZ(0,0,0)
        cls.stop = XYZ(2,2,2)
        cls.step = XYZ(1,1,1)

    def test_num_cubes(self):
        num = self.rh.num_cubes(self.start, self.stop, self.step)
        self.assertEqual(num, 8)

    def test_make_cubes(self):
        gen = self.rh.make_cubes(self.start, self.stop, self.step)

        self.assertEqual(type(gen), types.GeneratorType)

        cubes = list(gen)
        expected = [XYZ(0,0,0),
                    XYZ(0,0,1),
                    XYZ(0,1,0),
                    XYZ(0,1,1),
                    XYZ(1,0,0),
                    XYZ(1,0,1),
                    XYZ(1,1,0),
                    XYZ(1,1,1)]

        self.assertEqual(len(cubes), 8)
        self.assertEqual(cubes, expected)

class TestEnqueue(TestCaseWithPatchObject):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

        cls.name = 'downsample-1234'
        cls.arn = SQS_URL + cls.name
        cls.start = XYZ(0,0,0)
        cls.stop = XYZ(2,2,2)
        cls.step = XYZ(1,1,1)
        cls.chunk = [XYZ(0,0,0),
                     XYZ(1,1,1)]

    def test_populate(self):
        mock_enqueue_cubes = self.patch_object(self.rh, 'enqueue_cubes')
        count = self.rh.populate_cubes(self.arn, self.start, self.stop, self.step)

        self.assertEqual(count, 8)

        expected = [call(self.arn, [XYZ(0,0,0),
                                    XYZ(0,0,1),
                                    XYZ(0,1,0),
                                    XYZ(0,1,1)]),
                    call(self.arn, [XYZ(1,0,0),
                                    XYZ(1,0,1),
                                    XYZ(1,1,0),
                                    XYZ(1,1,1)])]
        self.assertEqual(mock_enqueue_cubes.mock_calls, expected)

    @patch.object(SESSION, 'resource')
    def test_enqueue(self, mResource):
        self.rh.enqueue_cubes(self.arn, self.chunk)

        func = mResource.return_value.Queue.return_value.send_messages
        expected = [call(Entries=[{'Id': str(id(self.chunk[0])),
                                   'MessageBody': '[0, 0, 0]'},
                                  {'Id': str(id(self.chunk[1])),
                                   'MessageBody': '[1, 1, 1]'}])]
        self.assertEqual(func.mock_calls, expected)

    @patch.object(SESSION, 'resource')
    def test_enqueue_multi_batch(self, mResource):
        chunk = range(15)
        self.rh.enqueue_cubes(self.arn, chunk)

        func = mResource.return_value.Queue.return_value.send_messages
        self.assertEqual(len(func.mock_calls), 2)

        # index 2 of a call is the kwargs for the call
        self.assertEqual(len(func.mock_calls[0][2]['Entries']), 10)
        self.assertEqual(len(func.mock_calls[1][2]['Entries']), 5)

    @patch.object(SESSION, 'resource')
    def test_enqueue_error(self, mResource):
        func = mResource.return_value.Queue.return_value.send_messages
        func.side_effect = Exception("Could not enqueue data")

        with self.assertRaises(self.rh.ResolutionHierarchyError):
            self.rh.enqueue_cubes(self.arn, self.chunk)

class TestInvoke(TestCaseWithPatchObject):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested 
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

    def test_launch(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=[5,4,3,2,1]))
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE)
        self.assertEqual(len(mock_check_queue.mock_calls), (5 + self.rh.ZERO_COUNT) * 2) # (5+) for cubes_arn, (*2) for both queues

    def test_launch_empty(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=0))
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE)
        self.assertEqual(len(mock_check_queue.mock_calls), self.rh.ZERO_COUNT * 2) # * 2, one for each queue

    def test_launch_dlq(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=1, cubes_arn=0))
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        count = 10

        with self.assertRaises(self.rh.FailedLambdaError):
            self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE)
        self.assertEqual(len(mock_check_queue.mock_calls), 1) # single dlq check_queue call

    def test_launch_relaunch(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        cubes_count = [5] * self.rh.UNCHANGING_LAUNCH + [4,3,2,1]
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=cubes_count))

        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mock_check_queue.mock_calls), (len(cubes_count) + self.rh.ZERO_COUNT) * 2) # (*2) for both queues

    def test_launch_relaunch_multi(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        cubes_count = [5] * self.rh.UNCHANGING_LAUNCH + [4] * self.rh.UNCHANGING_LAUNCH + [3,2,1]
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=cubes_count))

        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE + 2) # +2 relaunches
        self.assertEqual(len(mock_check_queue.mock_calls), (len(cubes_count) + self.rh.ZERO_COUNT) * 2) # (*2) for both queues

    def test_launch_throttle(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        self.patch_object(self.rh, 'update_visibility_timeout')
        cubes_count = [5] * self.rh.UNCHANGING_THROTTLE + [4,3,2,1]
        throttle_count = [2] * self.rh.UNCHANGING_THROTTLE + [2,2,2,1,0,0,0,0,0,0,0,0,0]
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=cubes_count))
        self.patch_object(self.rh, 'lambda_throttle_count', side_effect=throttle_count)

        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mock_check_queue.mock_calls), ((len(cubes_count) + self.rh.ZERO_COUNT) * 2) + 2) # (+2) for 2 extra dlq polls in throttling loop, (*2) for both queues

    def test_launch_no_throttle(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        cubes_count = [5] * self.rh.UNCHANGING_THROTTLE + [4,3,2,1]
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=cubes_count))

        count = 10
        self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mock_check_queue.mock_calls), (len(cubes_count) + self.rh.ZERO_COUNT) * 2) # (*2) for both queues

    def test_launch_unchanging_max(self):
        mock_invoke_lambdas = self.patch_object(self.rh, 'invoke_lambdas', return_value=0) 
        self.patch_object(self.rh, 'lambda_throttle_count', return_value=0)
        self.patch_object(self.rh, 'update_visibility_timeout')
        cubes_count = [5] * self.rh.UNCHANGING_MAX
        mock_check_queue = self.patch_object(self.rh, 'check_queue',
                                             side_effect=make_check_queue(dlq_arn=0, cubes_arn=cubes_count))

        with self.assertRaises(self.rh.ResolutionHierarchyError):
            count = 10
            self.rh.launch_lambdas(count, 'lambda_arn', {}, 'dlq_arn', 'cubes_arn',
                                DOWNSAMPLE_QUEUE_URL, RECEIPT_HANDLE)

        self.assertEqual(len(mock_invoke_lambdas.mock_calls), self.rh.POOL_SIZE + 1) # +1 relaunch
        self.assertEqual(len(mock_check_queue.mock_calls), len(cubes_count) * 2) # (*2) for both queues

    @patch.object(SESSION, 'client', autospec=True)
    def test_invoke(self, mClient):
        invoke = mClient.return_value.invoke

        count = 2
        self.rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

        expected = call(FunctionName = 'lambda_arn',
                        InvocationType = 'Event',
                        Payload = '{}')

        self.assertEqual(len(invoke.mock_calls), count)
        self.assertEqual(invoke.mock_calls[0], expected)

    @patch.object(SESSION, 'client', autospec=True)
    def test_invoke_error(self, mClient):
        invoke = mClient.return_value.invoke
        invoke.side_effect = Exception("Could not invoke lambda")

        with self.assertRaises(self.rh.ResolutionHierarchyError):
            count = 2
            self.rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

    @patch.object(SESSION, 'client', autospec=True)
    def test_invoke_dlq(self, mClient):
        invoke = mClient.return_value.invoke

        mock_check_queue = self.patch_object(self.rh, 'check_queue', return_value=1)
        with self.assertRaises(self.rh.ResolutionHierarchyError):
            count = 12
            self.rh.invoke_lambdas(count, 'lambda_arn', '{}', 'dlq_arn')

        self.assertEqual(len(invoke.mock_calls), 9)

class TestQueue(TestCaseWithPatchObject):
    """
    This class does not use SESSION defined at the top of this file because
    that session was created before moto mocks are put in place.
    """

    @classmethod
    def setUpClass(cls):
        cls.downsample_id = '1234'
        cls.name = 'downsample-{}'.format(cls.downsample_id)
        cls.url = SQS_URL + cls.name

    def configure(self):
        """
        Import module under test.  Done in here so that moto mocks can be
        established first.

        Returns:
            (Session): Mocked boto3 Session.
        """
        # Import the code to be tested
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        self.rh = rh

        session = boto3.session.Session(region_name=REGION)

        mock_aws = self.patch_object(self.rh, 'aws')
        mock_aws.get_session.return_value = session

        return session

    def assertSuccess(self):
        self.assertTrue(True)

    @mock_sqs
    def test_create_queue(self):
        self.configure()
        actual = self.rh.create_queue(self.name)

        self.assertEqual(self.url, actual)

    @mock_sqs
    def test_create_queue_already_exist(self):
        self.configure()
        # SQS create_queue will not error if the same queue tries to be created

        first = self.rh.create_queue(self.downsample_id)
        second = self.rh.create_queue(self.downsample_id)

        self.assertEqual(first, second)

    @mock_sqs
    def test_delete_queue(self):
        session = self.configure()
        session.client('sqs').create_queue(QueueName = self.name)

        self.rh.delete_queue(self.url)

        self.assertSuccess()

    @mock_sqs
    def test_delete_queue_doesnt_exist(self):
        self.configure()
        # delete_queue logs the error
        self.rh.delete_queue(self.url)

        self.assertSuccess()

    @mock_sqs
    def test_check_queue_doesnt_exist(self):
        self.configure()
        count = self.rh.check_queue(self.url)

        self.assertEqual(count, 0)

    @mock_sqs
    def test_check_queue_empty(self):
        self.configure()
        arn = self.rh.create_queue(self.downsample_id)

        count = self.rh.check_queue(arn)

        self.assertEqual(count, 0)

    @mock_sqs
    def test_check_queue_non_empty(self):
        session = self.configure()
        arn = self.rh.create_queue(self.downsample_id)
        session.client('sqs').send_message(QueueUrl = arn,
                                           MessageBody = 'message')
        session.client('sqs').send_message(QueueUrl = arn,
                                           MessageBody = 'message')

        count = self.rh.check_queue(arn)

        self.assertEqual(count, 2)

@patch.object(SESSION, 'client', autospec=True)
class TestThrottle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Import the code to be tested
        import resolution_hierarchy as rh
        rh.POOL_SIZE = 2
        rh.MAX_LAMBDA_TIME = rh.timedelta()
        rh.datetime = DateTime
        rh.Pool = MultiprocessingPool
        cls.rh = rh

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
        count = self.rh.lambda_throttle_count(arn)

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

        count = self.rh.lambda_throttle_count('lambda_arn')

        self.assertEqual(count, 0)

    def test_throttle_error(self, mClient):
        self.mock_client(mClient, return_data=None)

        count = self.rh.lambda_throttle_count('lambda_arn')

        self.assertEqual(count, -1)
