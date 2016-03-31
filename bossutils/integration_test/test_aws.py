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

import bossutils
from bossutils.aws import *
import unittest
import boto3
import os

if os.environ.get('NOSE_UNIT_TESTS_RUNNING') is None:
    run_tests = True
else:
    run_tests = False


@unittest.skipUnless(run_tests, 'Skipping integration tests.')
class TestAWSManager(unittest.TestCase):

    def test_creation(self):
        aws_mngr = get_aws_manager()

    def test_get_session(self):
        aws_mngr = get_aws_manager()

        assert isinstance(aws_mngr, bossutils.aws.AWSManager)
        session1 = aws_mngr.get_session()

        assert isinstance(session1, boto3.session.Session)
        session2 = aws_mngr.get_session()
        assert isinstance(session2, boto3.session.Session)
        aws_mngr.put_session(session1)

    def test_session_created_properly(self):
        aws_mngr = get_aws_manager()
        session = aws_mngr.get_session()
        try:
            assert session.profile_name == 'default'

            services = session.get_available_services()
            assert ('s3' in services) == True
            assert ('dynamodb' in services) == True

        finally:
            aws_mngr.put_session(session)

    def test_session_dynamo(self):
        aws_mngr = get_aws_manager()
        session = aws_mngr.get_session()
        try:
            db = session.resource('dynamodb')
            table = db.Table('bossmeta')
            assert table.table_name == 'bossmeta'
        finally:
            aws_mngr.put_session(session)
