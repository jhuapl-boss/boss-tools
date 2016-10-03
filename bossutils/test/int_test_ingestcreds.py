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

from bossutils.ingestcreds import IngestCredentials
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.ndqueue.uploadqueue import UploadQueue
import os
import unittest
import warnings

class TestIngestCreds(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Suppress warnings about Boto3's unclosed sockets.
        warnings.simplefilter('ignore') 

        # Use ndingest in test mode.
        os.environ['NDINGEST_TEST'] = '1'

        cls.job_id = 125
        cls.nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, cls.job_id)
        UploadQueue.createQueue(cls.nd_proj)
        cls.upload_queue = UploadQueue(cls.nd_proj)

    @classmethod
    def tearDownClass(cls):
        UploadQueue.deleteQueue(cls.nd_proj)

    def setUp(self):
        self.ingest = IngestCredentials()
        self.domain = 'test.boss.io'

    def test_create_delete_policy(self):
        """Test both creation and deletion of the policy.

        Both operations tested because both must be done together to restore
        the system to a clean state.
        """
        policy_doc = {
            "Version": "2012-10-17",
            "Id": "TestUploadQueuePolicy",
            "Statement": [
                {
                    "Sid": "ClientQueuePolicy",
                    "Effect": "Allow",
                    "Action": ["sqs:ReceiveMessage"],
                    # In actual use, supply a queue's ARN as the resource.
                    "Resource": "*"
                }
            ]
        }

        created = False

        try:
            actual = self.ingest.create_policy(policy_doc, self.job_id)
            created = True
        except Exception as e:
            print(e)
            raise
        finally:
            deleted = self.ingest.delete_policy(self.job_id)
            if created:
                self.assertTrue(deleted)

    def test_generate_remove_credentials(self):
        """Test both generation and removal of credentials.

        Both operations tested because both must be done together to restore
        the system to a clean state.
        """
        policy_doc = {
            "Version": "2012-10-17",
            "Id": "TestUploadQueuePolicy",
            "Statement": [
                {
                    "Sid": "ClientQueuePolicy",
                    "Effect": "Allow",
                    "Action": ["sqs:ReceiveMessage"],
                    "Resource": "\"{}\"".format(self.upload_queue.arn)
                }
            ]
        }

        creds_created = False

        try:
            arn = self.ingest.create_policy(policy_doc, self.job_id)
            creds = self.ingest.generate_credentials(self.job_id, arn)
            creds_created = True
            self.assertTrue('access_key' in creds)
            self.assertTrue('secret_key' in creds)
        except Exception as e:
            print(e)
            raise
        finally:
            if creds_created:
                self.ingest.remove_credentials(self.job_id)
            self.ingest.delete_policy(self.job_id)
            self.assertIsNone(self.ingest.get_credentials(self.job_id))

    def test_get_credentials_invalid(self):
        """Test result of getting non-existent credentials.
        """
        self.assertIsNone(self.ingest.get_credentials(self.job_id))

