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


"""
Note that the module under test (resolution_hierarchy.py) is imported inside
of the various test classes.  This must be done to ensure that none of the
boto3 clients are instantiated before the moto mocks are put in place.  Also,
environment variables for the AWS keys should be set to a bogus value such as
`testing` to ensure that no real AWS resources are accidentally used.

https://github.com/spulec/moto#how-do-i-avoid-tests-from-mutating-my-real-infrastructure
"""

import boto3
import json
from moto import mock_sqs
from ._test_case_with_patch_object import TestCaseWithPatchObject

REGION = 'us-east-1'

class TestCheckDownsampleQueue(TestCaseWithPatchObject):
    def setUp(self):
        self.sfn_arn = 'fake-arn'

    def configure(self):
        """
        Configure an individual test.

        Doesn't use setUp() because this has to run inside the test method for
        proper mocking of AWS resources.

        Member variables set:
            sut (check_downsample_queue()): Function being tested.
            sut_args (dict): Args to pass the tested function.
            url (str): Downsample queue URL.
            session (Session): Boto3 session.
            DownsampleStatus (DownsampleStatus): String constants.
        """
        import resolution_hierarchy as rh

        self.DownsampleStatus = rh.DownsampleStatus

        # System (function) under test.
        self.sut = rh.check_downsample_queue

        self.session = boto3.session.Session(region_name=REGION)

        mock_aws = self.patch_object(rh, 'aws')
        mock_aws.get_session.return_value = self.session

        resp = self.session.client('sqs').create_queue(QueueName='fake-downsample-queue')
        self.url = resp['QueueUrl']

        self.sut_args = { 'queue_url': self.url, 'sfn_arn': self.sfn_arn }

    @mock_sqs
    def test_no_msg_available(self):
        self.configure()
        exp = { 'start_downsample': False }
        actual = self.sut(self.sut_args)
        self.assertDictEqual(exp, actual)

    @mock_sqs
    def test_msg_available(self):
        self.configure()
        msg = {
            'db_host': 'thedb.boss',
            'channel_id': '2',
            'some-fake-keys': 'to verify msg contents',
            'are': 'included with in the dict',
            'returned': 'by check_downsample_queue()',
        }
        self.session.client('sqs').send_message(QueueUrl=self.url, MessageBody=json.dumps(msg))
        exp = {
            'start_downsample': True,
            'queue_url': self.url,
            'sfn_arn': self.sut_args['sfn_arn'],
            'status': self.DownsampleStatus.IN_PROGRESS,
            'msg': msg,
        }
        actual = self.sut(self.sut_args)
        # This is the non-deprecated way of doing self.assertDictContainsSubset().
        self.assertGreaterEqual(actual.items(), exp.items())
        self.assertIn('job_receipt_handle', actual)


class TestDeleteDownSampleJob(TestCaseWithPatchObject):
    def setUp(self):
        self.sfn_arn = 'fake-arn'

    def configure(self):
        """
        Configure an individual test.

        Doesn't use setUp() because this has to run inside the test method for
        proper mocking of AWS resources.

        Member variables set:
            sut (delete_downsample_job()): Function being tested.
            url (str): Downsample queue URL.
            session (Session): Boto3 session.
            DownsampleStatus (DownsampleStatus): String constants.
        """
        import resolution_hierarchy as rh

        self.DownsampleStatus = rh.DownsampleStatus

        # System (function) under test.
        self.sut = rh.delete_downsample_job

        self.session = boto3.session.Session(region_name=REGION)

        mock_aws = self.patch_object(rh, 'aws')
        mock_aws.get_session.return_value = self.session

        resp = self.session.client('sqs').create_queue(QueueName='fake-downsample-queue')
        self.url = resp['QueueUrl']

    @mock_sqs
    def test_delete_job(self):
        self.configure()
        msg = { 'foo': 'bar' }
        db_host = 'thedb.test.boss'
        sqs = self.session.client('sqs')
        sqs.send_message(QueueUrl=self.url, MessageBody=json.dumps(msg))
        resp = sqs.receive_message(QueueUrl=self.url)
        lookup_key = 'fake-lookup-key'
        args = {
            'queue_url': self.url,
            'job_receipt_handle': resp['Messages'][0]['ReceiptHandle'],
            'sfn_arn': self.sfn_arn,
            'db_host': db_host,
            'lookup_key': lookup_key,
        }
        exp = {
            'queue_url': self.url,
            'sfn_arn': self.sfn_arn,
            'lookup_key': lookup_key,
        }

        actual = self.sut(args)

        self.assertEqual(exp, actual)


class TestUpdateVisibilityTimeout(TestCaseWithPatchObject):
    def configure(self):
        """
        Configure an individual test.

        Doesn't use setUp() because this has to run inside the test method for
        proper mocking of AWS resources.

        Member variables set:
            sut (update_visibility_timeout()): Function being tested.
            url (str): Downsample queue URL.
            session (Session): Boto3 session.
        """
        import resolution_hierarchy as rh

        # System (function) under test.
        self.sut = rh.update_visibility_timeout

        self.session = boto3.session.Session(region_name=REGION)

        mock_aws = self.patch_object(rh, 'aws')
        mock_aws.get_session.return_value = self.session

        resp = self.session.client('sqs').create_queue(QueueName='fake-downsample-queue')
        self.url = resp['QueueUrl']

    @mock_sqs
    def test_update_timeout(self):
        self.configure()
        msg = { 'foo': 'bar' }
        sqs = self.session.client('sqs')
        sqs.send_message(QueueUrl=self.url, MessageBody=json.dumps(msg))
        resp = sqs.receive_message(QueueUrl=self.url)

        self.sut(self.url, resp['Messages'][0]['ReceiptHandle'])
