# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
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

# lambdafcns is a symbolic link to boss-tools/lambda.  Since lambda is a
# reserved word, this allows importing upload_enqueue_lambda.py without
# updating scripts responsible for deploying the lambda code.
from lambdafcns.upload_enqueue_lambda import download_from_s3, enqueue_msgs, parse_line, MAX_BATCH_MSGS
import boto3
from io import StringIO
import json
from ndingest.ndqueue.uploadqueue import UploadQueue
import os
import unittest
from unittest.mock import patch

TEST_FILE_DATA = b'test binary data'

class FakeBucket:
    def download_fileobj(self, key, fp):
        fp.write(TEST_FILE_DATA)

class TestUploadEnqueueLambda(unittest.TestCase):
    def test_download_from_s3(self):
        bucket = FakeBucket()
        try:
            local_filename = download_from_s3(bucket, 'my_bucket_key')
            with open(local_filename, 'r') as fp:
                actual = fp.read()
            self.assertEqual(TEST_FILE_DATA.decode('utf-8'), actual)
        finally:
            os.remove(local_filename)

    def test_parse_line_bad_line(self):
        header = { 'job_id': 1, 'upload_queue_arn': '', 'ingest_queue_arn': '' }
        line = 'one_column'
        with self.assertRaises(RuntimeError):
            parse_line(header, line)

    def test_parse_line(self):
        header = { 'job_id': 1, 'upload_queue_arn': '', 'ingest_queue_arn': '' }
        line = 'chunk_key, tile_key'
        expected = {
            'job_id': 1,
            'upload_queue_arn': '',
            'ingest_queue_arn': '',
            'tile_key': 'tile_key',
            'chunk_key': 'chunk_key'
        }

        actual = parse_line(header, line)

        self.assertEqual(expected, json.loads(actual))

    @patch('lambdafcns.upload_enqueue_lambda.UploadQueue', autospec=True)
    @patch('lambdafcns.upload_enqueue_lambda.parse_line', autospec=True)
    def test_enqueue_msgs_less_than_10(self, fake_parse_line, fake_upload_queue):
        """Test with less than a full batch of messages.
        """

        # Create fake file with StringIO.
        s = StringIO()
        # Add header.
        s.write('{"job_id": 20}\n')
        # Add data lines (expect 2 msgs).
        s.write('chunk_key1, tile_key1\n')
        s.write('chunk_key2, tile_key2\n')
        s.seek(0)

        upload_queue_instance = fake_upload_queue.return_value
        fake_parse_line.return_value = '{"tile_key": "hash&10&20&30&0&5&6&7&0"}'

        # Function under test.
        enqueue_msgs(s)

        all_args = upload_queue_instance.sendBatchMessages.call_args
        args, kwargs = all_args

        expNumMsgs = 2
        self.assertEqual(expNumMsgs, len(args[0]))


    @patch('lambdafcns.upload_enqueue_lambda.UploadQueue', autospec=True)
    @patch('lambdafcns.upload_enqueue_lambda.parse_line', autospec=True)
    def test_enqueue_msgs_greater_than_10(self, fake_parse_line, fake_upload_queue):
        """Test with a full batch of messages plus one more.
        """

        # Create fake file with StringIO.
        s = StringIO()
        # Add header.
        s.write('{"job_id": 20}\n')
        # Add data lines (expect 2 msgs).
        s.write('chunk_key1, tile_key1\n')
        s.write('chunk_key2, tile_key2\n')
        s.write('chunk_key3, tile_key3\n')
        s.write('chunk_key4, tile_key4\n')
        s.write('chunk_key5, tile_key5\n')
        s.write('chunk_key6, tile_key6\n')
        s.write('chunk_key7, tile_key7\n')
        s.write('chunk_key8, tile_key8\n')
        s.write('chunk_key9, tile_key9\n')
        s.write('chunk_key10, tile_key10\n')
        s.write('chunk_key11, tile_key11\n')
        s.seek(0)

        upload_queue_instance = fake_upload_queue.return_value
        fake_parse_line.return_value = '{"tile_key": "hash&10&20&30&0&5&6&7&0"}'

        # Function under test.
        enqueue_msgs(s)

        all_args_list = upload_queue_instance.sendBatchMessages.call_args_list

        args0, _ = all_args_list[0]
        args1, _ = all_args_list[1]

        expNumMsgs0 = MAX_BATCH_MSGS
        expNumMsgs1 = 1
        self.assertEqual(expNumMsgs0, len(args0[0]))
        self.assertEqual(expNumMsgs1, len(args1[0]))
