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

"""Test the enqueue cuboid ids lambda
"""
import unittest

import boto3, json
from moto import mock_sqs
from lambdafcns.enqueue_cuboid_ids_lambda import handler, create_messages, event_fields, build_retry_response, build_batch_entries

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

        resp = self.session.client('sqs').create_queue(QueueName='fake-downsample-queue')
        self.url = resp['QueueUrl']
    
    def get_fake_event(self, ids, n, url=None):
        e = { f : "dummy" for f in event_fields }
        e['attempt'] = 1
        e['config'] = {'dummy':'dummy'}
        if url:
            e['sqs_url'] = url
        e['ids'] = ids
        e['num_ids_per_msg'] = n
        return { k : e[k] for k in e if e[k] }

    def test_partial_event(self):
        # empty event
        self.assertRaisesRegex(ValueError, "Missing or empty event", handler, None)
        # event missing control parameters
        e = self.get_fake_event(None, None)
        self.assertRaisesRegex(KeyError, "Missing keys: .*", handler, e)
        # event missing the num_ids_per_msg field
        e['ids'] = range(10)
        self.assertRaisesRegex(KeyError, "Missing keys: num_ids_per_msg", handler, e)
        # event with wrong type for num_ids_per_msg
        e['num_ids_per_msg'] = "dummy"
        self.assertRaisesRegex(TypeError, ".*Expected num_ids_per_msg.*", handler, e)
        # event with wrong type for ids 
        e['num_ids_per_msg'] = 10
        e['ids'] = "dummy"
        self.assertRaisesRegex(TypeError, ".*Expected ids.*", handler, e)
 
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

    def test_retry_response(self):
        totalIds = 91
        idsPerMessage = 5
        print(f"Testing retry response with {totalIds} Ids and {idsPerMessage} Ids per message")
        e = self.get_fake_event(range(totalIds),idsPerMessage)
        msgs = create_messages(e)
        batch = build_batch_entries(msgs)
        print(f"created a batch of {len(batch)} messages")
        failed = [e for i,e in enumerate(batch) if i%2 == 1]
        print(f"failed {len(failed)} messages in the batch")
        unsentFromBatch = len([i for m in failed for i in json.loads(m['MessageBody'])['id_group']])
        print(f"There are {unsentFromBatch} Ids that have failed to send")
        numUnsentIds = totalIds - (len(batch) * idsPerMessage) + unsentFromBatch
        print(f"There are a total of {numUnsentIds} unsent Ids")
        response = build_retry_response(failed, msgs, 1)
        self.assertEqual(len(response['ids']), numUnsentIds)

    @mock_sqs
    def test_sqs_queue(self):
        self.configure()
        sqs = boto3.resource('sqs')
        e = self.get_fake_event(range(91),10,self.url)
        response = handler(e)
        self.assertFalse(response)
