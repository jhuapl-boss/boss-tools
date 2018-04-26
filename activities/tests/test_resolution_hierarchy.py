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

import sys
import importlib
import json
import unittest
from unittest.mock import patch, MagicMock, call

import boto3
from moto import mock_sqs, mock_dynamodb, mock_lambda

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
path = "/home/microns/repositories/boss-tools/bossutils" #TODO calculate path
sys.path.insert(0, path)
md = importlib.import_module('multidimensional')
sys.modules['bossutils.multidimensional'] = md
sys.path.pop(0)

# Import the code to be tested 
import resolution_hierarchy as rh
rh.MAX_LAMBDA_TIME = rh.timedelta()


"""
Test isotropic normal
Test anisotropic normal
Test anisotropic at the isotropic split
Test anisotropic after the isotropic split

Test passing in downsample_id
Always create / delete the queue or create it when creating the downsample_id and delete when res_lt_max is False?
Test non even frame size (frame size is not an even multiple of cube size)
Test non zero frame start (should fail, want to make sure we understand the problem and it doesn't change)
Test resolution / resolution_max logic so it doesn't change



"""

@patch.object(rh, 'delete_queue')
@patch.object(rh, 'launch_lambda')
@patch.object(rh, 'bucket')
@patch.object(rh, 'make_args')
@patch.object(rh, 'create_queue', return_value = SQS_URL + 'downsample-1234')
@patch.object(rh.random, 'random', return_value = 1234)
class TestDownsampleChannel(unittest.TestCase):
    def get_args(self, scale=2, **kwargs):
        cube_size = md.XYZ(*CUBOIDSIZE()[0])
        frame_stop = cube_size * scale
        args = {
            # Only including keys that the Activity uses, the others are passed
            # to the downsample_volume lambda without inspection
            'downsample_volume_lambda': 'lambda-arn',
            'downsample_status_table': 'statue-table',

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

    def test_downsample_channel_iso(self, mRandom, mCreateQueue, mMakeArgs, mBucket, mLaunchLambda, mDeleteQueue):
        args1 = self.get_args(type='isotropic')

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['downsample_id'], '1234')

        expected = call(args2['downsample_id'])
        self.assertEqual(mCreateQueue.mock_calls, [expected])

        expected = call(args1,
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,2),
                        md.XYZ(512, 512, 16),
                        False)
        self.assertEqual(mMakeArgs.mock_calls, [expected])

        expected = call(mMakeArgs.return_value, rh.BUCKET_SIZE)
        self.assertEqual(mBucket.mock_calls, [expected])

        expected = call('lambda-arn',
                        mCreateQueue.return_value,
                        'statue-table',
                        args2['downsample_id'],
                        mBucket.return_value)
        self.assertEqual(mLaunchLambda.mock_calls, [expected])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 16)

        self.assertEqual(args2['resolution'], 1)
        self.assertTrue(args2['res_lt_max'])

        expected = call(mCreateQueue.return_value)
        self.assertEqual(mDeleteQueue.mock_calls, [expected])

    def test_downsample_channel_aniso(self, mRandom, mCreateQueue, mMakeArgs, mBucket, mLaunchLambda, mDeleteQueue):
        args1 = self.get_args(type='anisotropic')

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertEqual(args2['downsample_id'], '1234')

        expected = call(args2['downsample_id'])
        self.assertEqual(mCreateQueue.mock_calls, [expected])

        expected = call(args1,
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,1),
                        md.XYZ(512, 512, 16),
                        False)
        self.assertEqual(mMakeArgs.mock_calls, [expected])

        expected = call(mMakeArgs.return_value, rh.BUCKET_SIZE)
        self.assertEqual(mBucket.mock_calls, [expected])

        expected = call('lambda-arn',
                        mCreateQueue.return_value,
                        'statue-table',
                        args2['downsample_id'],
                        mBucket.return_value)
        self.assertEqual(mLaunchLambda.mock_calls, [expected])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 32)

        self.assertNotIn('iso_x_start', args2)

        self.assertEqual(args2['resolution'], 1)
        self.assertTrue(args2['res_lt_max'])

        expected = call(mCreateQueue.return_value)
        self.assertEqual(mDeleteQueue.mock_calls, [expected])

    def test_downsample_channel_aniso_split(self, mRandom, mCreateQueue, mMakeArgs, mBucket, mLaunchLambda, mDeleteQueue):
        args1 = self.get_args(type='anisotropic', iso_resolution=1)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        self.assertIn('iso_x_start', args2)

        self.assertEqual(args2['iso_x_stop'], 512)
        self.assertEqual(args2['iso_y_stop'], 512)
        self.assertEqual(args2['iso_z_stop'], 32)

    def test_downsample_channel_aniso_post_split(self, mRandom, mCreateQueue, mMakeArgs, mBucket, mLaunchLambda, mDeleteQueue):
        args1 = self.get_args(type='anisotropic',
                              iso_resolution=0,
                              iso_x_start = 0, iso_y_start = 0, iso_z_start = 0,
                              iso_x_stop = 1024, iso_y_stop = 1024, iso_z_stop = 32)

        args2 = rh.downsample_channel(args1) # warning, will mutate args1 === args2

        for call in mMakeArgs.mock_calls:
            print(call)

        expected = call(args1,
                        md.XYZ(0,0,0),
                        md.XYZ(2,2,2),
                        md.XYZ(2,2,1),
                        md.XYZ(512, 512, 16),
                        False)
        expected1 = call(args1,
                         md.XYZ(0,0,0),
                         md.XYZ(2,2,2),
                         md.XYZ(2,2,2),
                         md.XYZ(512, 512, 16),
                         True)
        self.assertEqual(mMakeArgs.mock_calls, [expected, expected1])

        self.assertEqual(args2['x_stop'], 512)
        self.assertEqual(args2['y_stop'], 512)
        self.assertEqual(args2['z_stop'], 32)

        self.assertEqual(args2['iso_x_stop'], 512)
        self.assertEqual(args2['iso_y_stop'], 512)
        self.assertEqual(args2['iso_z_stop'], 32)



class TestArgumentGeneration(unittest.TestCase):
    def test_make_args(self):
        start = md.XYZ(0,0,0)
        stop = md.XYZ(2,2,2)
        step = md.XYZ(1,1,1)

        args = list(rh.make_args('args', start, stop, step, 'dim', 'use_iso_flag'))

        expected = {
            'args': 'args',
            'target': start,
            'step': step,
            'dim': 'dim',
            'use_iso_flag': 'use_iso_flag',
        }

        self.assertEqual(len(args), 8)
        self.assertEqual(args[0], expected)

    def test_bucket_exact(self):
        # input is a generator
        sub_args = (i for i in [1,2,3,4])

        buckets = list(rh.bucket(sub_args, 2)) # buckets of size 2

        expected = {
            'lambda-name': 'downsample_volume',
            'bucket_args': [1,2],
        }

        self.assertEqual(len(buckets), 2)
        self.assertEqual(buckets[0], expected)

    def test_bucket_not_exact(self):
        # input is a generator
        sub_args = (i for i in [1,2,3,4,5])

        buckets = list(rh.bucket(sub_args, 2)) # buckets of size 2

        expected = {
            'lambda-name': 'downsample_volume',
            'bucket_args': [5],
        }

        self.assertEqual(len(buckets), 3)
        self.assertEqual(buckets[-1], expected)

@patch.object(rh, 'check_queue', return_value = 0)
@patch.object(SESSION, 'client')
class TestLaunchLambda(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lambda_arn = 'downsample-volume'
        cls.downsample_id = '1234'
        cls.queue_arn = SQS_URL + 'downsample-1234'
        cls.buckets = [{
            'lambda-name': 'downsample_volume',
            'bucket_args': [{
                'args': {
                    'foo': 'foo',
                    'bar': 'bar',
                },
                'target': md.XYZ(1,1,1),
                'step': '', # Not used
                'dim': '', # Not used
                'use_iso_flag': False
            }]
        }, {
            'lambda-name': 'downsample_volume',
            'bucket_args': [{
                'args': {
                    'foo': 'foo',
                    'bar': 'bar',
                },
                'target': md.XYZ(2,2,2),
                'step': '', # Not used
                'dim': '', # Not used
                'use_iso_flag': False
            }]
        }]

    def mock_clients(self, client):
        clients = {
            'lambda': MockLambda(),
        }

        def client_(name):
            if name not in clients:
                clients[name] = MagicMock()
            return clients[name]

        client.side_effect = client_
        return clients

    def test_launch_lambda(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        rh.launch_lambda(self.lambda_arn,
                         self.queue_arn,
                         '', # Not used
                         self.downsample_id,
                         self.buckets)

        expected = [
            call(FunctionName = self.lambda_arn,
                 InvocationType = 'Event',
                 Payload = json.dumps(bucket).encode('UTF8'))
            for bucket in self.buckets
        ]
        self.assertEqual(mClients['lambda'].invoke.mock_calls, expected)

        self.assertEqual(len(mCheckQueue.mock_calls), 3) # Check after each lambda launch,
                                                         # plus a final check

    def test_launch_lambda_unknown_exception(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        ex = mClients['lambda'].exceptions.UnknownException
        mClients['lambda'].invoke.side_effect = ex()

        with self.assertRaises(rh.FailedLambdaError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 1)
        self.assertEqual(len(mCheckQueue.mock_calls), 0)

    def test_launch_lambda_too_large_exception(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        ex = mClients['lambda'].exceptions.RequestTooLargeException
        mClients['lambda'].invoke.side_effect = ex()

        with self.assertRaises(rh.LambdaLaunchError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 1)
        self.assertEqual(len(mCheckQueue.mock_calls), 0)

    @patch.object(rh, 'RETRY_LIMIT', 0)
    def test_launch_lambda_too_quick(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        # Slowdown twice and then finish
        ex = mClients['lambda'].exceptions.EC2ThrottledException
        mClients['lambda'].invoke.side_effect = [ex(), ex(), None, None]

        rh.launch_lambda(self.lambda_arn,
                         self.queue_arn,
                         '', # Not used
                         self.downsample_id,
                         self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 4) # 2 retries + 2 success
        self.assertEqual(len(mCheckQueue.mock_calls), 3)

    @patch.object(rh, 'RETRY_LIMIT', 1)
    def test_launch_lambda_too_quick_exception(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        # Slowdown twice and then finish
        ex = mClients['lambda'].exceptions.EC2ThrottledException
        mClients['lambda'].invoke.side_effect = [ex(), ex(), ex()]

        with self.assertRaises(rh.LambdaRetryLimitExceededError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 3)
        self.assertEqual(len(mCheckQueue.mock_calls), 0)

    @patch.object(rh, 'RETRY_LIMIT', 0)
    def test_launch_lambda_resource_limit(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        # Slowdown twice and then finish
        ex = mClients['lambda'].exceptions.TooManyRequestsException
        mClients['lambda'].invoke.side_effect = [ex(), ex(), None, None]

        rh.launch_lambda(self.lambda_arn,
                         self.queue_arn,
                         '', # Not used
                         self.downsample_id,
                         self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 4) # 2 retries + 2 success
        self.assertEqual(len(mCheckQueue.mock_calls), 3)

    @patch.object(rh, 'RETRY_LIMIT', 1)
    def test_launch_lambda_resource_limit_exception(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)

        # Slowdown twice and then finish
        ex = mClients['lambda'].exceptions.TooManyRequestsException
        mClients['lambda'].invoke.side_effect = [ex(), ex(), ex()]

        with self.assertRaises(rh.LambdaRetryLimitExceededError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 3)
        self.assertEqual(len(mCheckQueue.mock_calls), 0)

    def test_launch_lambda_dlq_msg(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)
        mCheckQueue.return_value = 1

        with self.assertRaises(rh.FailedLambdaError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 1)
        self.assertEqual(len(mCheckQueue.mock_calls), 1)

    def test_launch_lambda_dlq_msg_2(self, mClient, mCheckQueue):
        mClients = self.mock_clients(mClient)
        mCheckQueue.side_effect = [0, 0, 1]

        with self.assertRaises(rh.FailedLambdaError):
            rh.launch_lambda(self.lambda_arn,
                             self.queue_arn,
                             '', # Not used
                             self.downsample_id,
                             self.buckets)

        self.assertEqual(len(mClients['lambda'].invoke.mock_calls), 2)
        self.assertEqual(len(mCheckQueue.mock_calls), 3)



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
        actual = rh.create_queue(self.downsample_id)

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

class TestErrorState(unittest.TestCase):
    def test_error_state(self):
        with self.assertRaises(rh.FailedLambdaError):
            NOUSE = ''
            rh.error_state(NOUSE, NOUSE, NOUSE, ex=None)

    def test_error_state_exception(self):
        with self.assertRaises(ValueError):
            NOUSE = ''
            rh.error_state(NOUSE, NOUSE, NOUSE, ex=ValueError())
