# Copyright 2021 The Johns Hopkins University Applied Physics Laboratory
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

# test strategy
# 1. Properly split the ids across multiple messages in the queue
# 2. Handle failures correctly. Expected failures are a) network, 
# 3. SUT uses the batch enque 

import unittest

import boto3, json
from moto import mock_sqs
from lambdafcns.enqueue_cuboid_ids_lambda import handler, create_messages, event_fields

REGION = 'us-east-1'

class TestEnqueueCuboidIds(unittest.TestCase):
    def setUp(self):
        self.sfn_arn = 'fake-arn'
    
    def configure(self):
        """Configures this test to use mock AWS objects.

        Doesn't use setUp() because this has to run inside the test method for
        proper mocking of AWS resources.

        Member variables set:
            sut (check_downsample_queue()): Function being tested.
            sut_args (dict): Args to pass the tested function.
            url (str): Downsample queue URL.
            session (Session): Boto3 session.
            DownsampleStatus (DownsampleStatus): String constants.
        """
        self.session = boto3.session.Session(region_name=REGION)

 #       mock_aws = self.patch_object(rh, 'aws')
 #       mock_aws.get_session.return_value = self.session

        resp = self.session.client('sqs').create_queue(QueueName='fake-downsample-queue')
        self.url = resp['QueueUrl']

        self.sut_args = { 'queue_url': self.url, 'sfn_arn': self.sfn_arn }
    
    def get_fake_event(self, ids, n):
        e = { f : "dummy" for f in event_fields }
        e['ids'] = ids
        e['num_ids_per_msg'] = n
        return { k : e[k] for k in e if e[k] }

    @mock_sqs
    def test_sqs_queue(self):
        self.configure()
        sqs = boto3.resource('sqs')
        queue = sqs.Queue(self.url)
        messages = [ {'Id': str(i), 'MessageBody': "hello", 'DelaySeconds':0} for i in range(5)]
        resp = queue.send_messages(Entries=messages)
        print(resp)

    def test_partial_event(self):
        # empty event
        self.assertRaisesRegex(ValueError, "Missing event data", handler, None)
        # event missing control parameters
        e = self.get_fake_event(None, None)
        self.assertRaisesRegex(KeyError, "Missing keys: .*", handler, e)
        # event missing the num_ids_per_msg field
        e['ids'] = range(10)
        self.assertRaisesRegex(KeyError, "Missing keys: num_ids_per_msg", handler, e)
        # event with wrong type for num_ids_per_msg
        e['num_ids_per_msg'] = "dummy"
        self.assertRaisesRegex(TypeError, "Expected int.*", handler, e)
        # event with wrong type for ids 
        e['num_ids_per_msg'] = 10
        e['ids'] = "dummy"
        self.assertRaisesRegex(TypeError, "Expected list.*", handler, e)
 
    def test_message_splitting(self):
        # event with 91 IDs should result in 10 messages
        e = self.get_fake_event(range(91),10)
        msgs = create_messages(e)
        n = 0 # omits warning for final assertion
        for n,m in enumerate(msgs):
            msg = json.loads(m)
            # each message should have less than or equal to num_ids_per_msg IDs
            self.assertLessEqual(len(msg['id_group']), e['num_ids_per_msg'])
        # enumerator is 0 based
        self.assertEqual(n+1, 10)
        