import bossutils
from bossutils.aws import *
import unittest


# TODO: Clean up/make tests better once cred issuing stuff has been clarified and possibly changed
class TestAWSManager(unittest.TestCase):

    def test_creation(self):
        aws_mngr = get_aws_manager()

    def test_get_session(self):
        aws_mngr = get_aws_manager()

        assert isinstance(aws_mngr, bossutils.aws.AWSManager)
        session1 = aws_mngr.get_session()

        assert isinstance(session1, bossutils.aws.Boto3Session)
        session2 = aws_mngr.get_session()
        assert isinstance(session2, bossutils.aws.Boto3Session)
        aws_mngr.put_session(session1)

    def test_session_created_properly(self):
        aws_mngr = get_aws_manager()
        boto3 = aws_mngr.get_session()
        try:
            assert boto3.session.profile_name == 'default'
        finally:
            aws_mngr.put_session(boto3)

    def test_session_dynamo(self):
        aws_mngr = get_aws_manager()
        boto3 = aws_mngr.get_session()
        try:
            table = boto3.session.Table('bossmeta')
            assert table.table_name == 'bossmeta'
        finally:
            aws_mngr.put_session(boto3)



