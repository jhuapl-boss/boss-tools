#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Johns Hopkins University Applied Physics Laboratory
# No License - Closed source
import boto3
from bossutils import configuration, credentials
from bossutils.logger import BossLogger
import multiprocessing
import queue


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
        self.__credentials = None

        if config['aws_mngr']['num_sessions'] == 'auto':
            self.__num_sessions = multiprocessing.cpu_count()
        else:
            self.__num_sessions = config['aws_mngr']['num_sessions']

        # Initialize the credentials and sessions
        self.__init_sessions()

    def __get_credentials(self):
        """
        Private method to query the credential service for for AWS credentials
        :return: None
        """
        blog = BossLogger().logger
        blog.info("AWSManager - Getting credentials from service")

        # Get credentials
        creds = credentials.get_credentials()

        # Finish updating the class
        if creds:
            self.__credentials = creds
            blog = BossLogger().logger
            blog.info("AWSManager - Successfully updated AWS credentials")
        else:
            blog.error("AWSManager - Failed to acquire AWS credentials")

    def __init_sessions(self):
        """
        Private method to initialize the instance by getting credentials and creating a pool of sessions
        :return:
        """
        # Get new credentials
        self.__get_credentials()

        # Create Sessions and store in class
        for session in range(0, self.__num_sessions):
            self.__create_session()

    def __create_session(self):
        """
        Method to create a new boto3.session.Session object and add it to the pool
        :return: None
        """
        temp_session = boto3.session.Session(aws_access_key_id=self.__credentials["access_key"],
                                             aws_secret_access_key=self.__credentials["secret_key"],
                                             region_name=self.region)
        
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




