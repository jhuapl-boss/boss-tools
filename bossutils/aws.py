#!/usr/bin/env python
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

import boto3
from urllib.error import URLError
from bossutils import configuration, utils
from bossutils.logger import BossLogger
import multiprocessing
import queue
import os
import json
from datetime import datetime


def get_region():
    """
    Return the  aws region based on the machine's meta data

    If mocking with moto, metadata is not supported and "us-east-1" is always returned

    Returns: aws region

    """
    if 'LOCAL_DYNAMODB_URL' in os.environ:
        # If you get here, you are testing locally
        return "us-east-1"
    else:
        try:
            region = utils.read_url(utils.METADATA_URL + 'placement/availability-zone')[:-1]
            return region
        except NotImplementedError:
            # If you get here, you are mocking and metadata is not supported.
            return "us-east-1"
        except URLError:
            return None


def get_session():
    """
    Returns a boto3 session with the region set to be the current region
    Returns:

    """
    return boto3.session.Session(region_name=get_region())

# DP NOTE: StepFunction methods adapted from heaviside library
def sfn_execute(session, name, data = None):
    """Start executing a StepFunction

    Args:
        session (Session): Boto3 session
        name (string): Name of the StepFunction to execute
        input_ (Json): Json input data for the first state to process

    Returns:
        string: ARN of the state machine execution, used to get status and output data
    """
    client = session.client('stepfunctions')

    arn = None
    resp = client.list_state_machines()
    for machine in resp['stateMachines']:
        if machine['name'] == name:
            arn = machine['stateMachineArn']

    if arn is None:
        raise Exception("StepFunction '{}' doesn't exist".format(name))

    input_ = json.dumps(input_)
    name = name + "-" + datetime.now().strftime("%Y%m%d%H%M%s%f")

    resp = client.start_execution(stateMachineArn = arn,
                                  name = name,
                                  input = input_)
    arn = resp['executionArn']
    return arn

def sfn_status(session, arn):
    """Get the status of a StepFunction execution

    Args:
        session (Session): Boto3 session
        arn (string): ARN of the execution to get the status of

    Returns:
        string: One of 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'
    """
    client = session.client('stepfunctions')
    resp = client.describe_execution(executionArn = arn)
    return resp['status']

def sfn_result(session, arn, wait=10):
    """Get the results of a StepFunction execution

    Args:
        session (Session): Boto3 session
        arn (string): ARN of the execution to get the results of
        wait (int): Seconds to wait between polling

    Returns:
        dict|None: Dict of Json data or
                   None if there was an error getting the failure output
    """
    client = session.client('stepfunctions')

    while True:
        resp = client.describe_execution(executionArn = arn)
        if resp['status'] != 'RUNNING':
            if 'output' in resp:
                return json.loads(resp['output'])
            else:
                resp = client.get_execution_history(executionArn = arn,
                                                    reverseOrder = True)
                event = resp['events'][0]
                for key in ['Failed', 'Aborted', 'TimedOut']:
                    key = 'execution{}EventDetails'.format(key)
                    if key in event:
                        return event[key]
                return None
        else:
            time.sleep(wait)

class AWSManager:
    """
    Class to manage a pool of AWS boto3 sessions in a thread-safe manner.

    AWSManager gets temporary AWS user credentials from the credential service. On creation it spins up a pool of
    boto3 sessions.

    If ['aws_mngr']['num_sessions'] in boss.config is set to "auto", the pool size is set to the number of cores on the
    current server.  This is in order to match the number of nginx worker threads, which typically matchs the number of
    cores. Otherwise, set ['aws_mngr']['num_sessions'] to an integer indicating how many sessions to start.

    The AWSManager gets configured from the boss.conf file which is stored in the boss-tools repository and installed
    by the deployment software.  It is located at /etc/boss/boss.config

    These sessions are accessible via a globally available generator "get_aws_manager()"

    :ivar region: the AWS region, currently set to us-east-1 by default if omitted
    """

    def __init__(self, region='us-east-1'):
        # Load boss config file
        config = configuration.BossConfig()

        # Set Properties
        self.region = region
        self.__sessions = queue.Queue()

        if config['aws_mngr']['num_sessions'] == 'auto':
            self.__num_sessions = multiprocessing.cpu_count()
        else:
            self.__num_sessions = config['aws_mngr']['num_sessions']

        # Initialize the credentials and sessions
        self.__init_sessions()

    def __init_sessions(self):
        """
        Private method to initialize the instance by getting credentials and creating a pool of sessions
        :return:
        """
        # Create Sessions and store in class
        for session in range(0, self.__num_sessions):
            self.__create_session()

    def __create_session(self):
        """
        Method to create a new boto3.session.Session object and add it to the pool
        :return: None
        """
        temp_session = get_session()
        
        blog = BossLogger().logger
        blog.info("AWSManager - Created new boto3 session and added to the pool")
        self.__sessions.put(temp_session)

    def get_session(self):
        """
        Method to get a boto3.session.Session object.

        If session credentials have expired a new session is created.

        If no sessions are available (if a previous consumer of a session didn't return it for some reason, e.g. an
        exception occurred), a new session is created

        :return: boto3.session.Session
        """
        temp_session = None
        while not temp_session:
            try:
                temp_session = self.__sessions.get(block=False)

            except queue.Empty:
                # No session was available so generate one
                blog = BossLogger().logger        
                blog.info("AWSManager - No session was available while trying to execute get_session.  Dynamically creating a new session.")
                self.__create_session()

        return temp_session

    def put_session(self, session):
        """
        Method to return a session to the session pool

        :param session: boto3.session.Session
        :return: None
        """
        self.__sessions.put(session)


def _aws_manager():
    """
    Private function that implements a generator to return the global AWSManager instance.

    :returns: Global AWSManager instance
    :rtype: bossutils.aws.AWSManager
    """
    yield None
    aws_mngr = AWSManager()
    while True:
        yield aws_mngr


# GLOBAL AWS MANAGER INSTANCE - Random name for namespace safety
_AWS_MNGR_SDFSDNNFJBASFSGW = _aws_manager()


def get_aws_manager():
    """
    Generator function to access the global AWSManager instance.

    :returns: The global AWSManager
    :rtype: bossutils.aws.AWSManager
    """
    aws_mngr = next(_AWS_MNGR_SDFSDNNFJBASFSGW)
    if aws_mngr:
        return aws_mngr
    else:
        return get_aws_manager()




