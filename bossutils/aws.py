#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Johns Hopkins University Applied Physics Laboratory
# No License - Closed source
import boto3
from bossutils import vault
from bossutils import utils, configuration, logger
import multiprocessing

import threading
import queue
import time


class Boto3Session:
    def __init__(self, _session, _cred_id):
        """
        A class to store a boto3 session and credential_id.  This lets the AWS manager automatically renew sessions
        with stale credentials

        :param _session: a boto3 session
        :param _cred_id: an integer indicating the credential version
        :return:
        """
        # Properties
        self.session = _session
        self.credential_id = _cred_id


class AWSManager:
    """
    Class to handle AWS boto3 sessions automagically in a thread-safe manner.

    AWSManager gets temporary AWS user credentials from vault when instantiated. On creation it spins up a pool of
    boto3 sessions.

    If ['aws_mngr']['num_sessions'] in boss.config is set to "auto", the pool size is set to the number of cores on the
    current server.  This is in order to match the number of nginx worker threads, which should match the number of
    cores. Otherwise, set ['aws_mngr']['num_sessions'] to an integer indicating how many sessions to start.

    The AWSManager gets configured from the boss.conf file which is stored in the boss-manage repository and installed
    by the deployment software.  It is located at /etc/boss/boss.config

    These sessions are accessible via a globally available generator "get_aws_manager()"

    :ivar region: the AWS region, currently set to us-east-1
    """

    def __init__(self):
        # Load boss config file
        config = configuration.BossConfig()

        # Set Properties
        #self.region = utils.read_url(utils.METADATA_URL + "placement/availability-zone")
        self.region = 'us-east-1'
        self.__sessions = queue.Queue()
        self.__credential_lifespan = int(config['aws_mngr']['cred_lifespan']) - (5 * 60)  # Expire creds 5min early
        self.__credential_set_id = 0
        self.__credentials = None

        if config['aws_mngr']['num_sessions'] == 'auto':
            self.__num_sessions = multiprocessing.cpu_count()
        else:
            self.__num_sessions = config['aws_mngr']['num_sessions']

        # Initialize the credentials and sessions
        self.__init_sessions()
        self.__update_timer = None

    def __del__(self):
        """
        Explicitly cancel the update timer if it is running
        :return:
        """
        if self.__update_timer:
            self.__update_timer.cancel()
            print("Cleaned up timer")

    def __get_credentials(self):
        """
        Private method to query vault for AWS credentials
        :return: None
        """
        blog = logger.BossLogger()
        blog.info("AWSManager - Requesting a new set of credentials")

        # Get credentials
        v = vault.Vault()
        path = "aws/creds/" + v.config["system"]["type"]
        temp_credentials = v.read_dict(path)

        # Wait for credentials to be live
        time.sleep(15)

        # Finish updating the class
        self.__credentials = temp_credentials
        self.__credential_set_id += self.__credential_set_id
        blog = logger.BossLogger()
        blog.info("AWSManager - Successfully updated AWS credentials")

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

    def __refresh_credentials(self):
        """
        Private method to renew the ASW credentials.  Restarts the credential refresh timer.
        :return: None
        """
        # Get new credentials
        self.__get_credentials()

        # Set timer for refresh
        self.__update_timer = threading.Timer(self.__credential_lifespan, self.__refresh_credentials)
        self.__update_timer.start()

    def __create_session(self):
        """
        Method to create a new boto3 session and add it to the pool
        :return: None
        """
        temp_session = Boto3Session(boto3.session.Session(aws_access_key_id=self.__credentials["access_key"],
                                                          aws_secret_access_key=self.__credentials["secret_key"],
                                                          region_name=self.region),
                                    self.__credential_set_id)

        blog = logger.BossLogger()
        blog.info("AWSManager - Created new boto3 session and added to the pool")
        self.__sessions.put(temp_session)

    def start_credential_refresh(self):
        """
        Method to start the background credential refresh thread
        :return:
        """
        # Create credential refresh timer
        self.__update_timer = threading.Timer(self.__credential_lifespan, self.__refresh_credentials)
        self.__update_timer.start()
        blog = logger.BossLogger()
        blog.info("AWSManager - Started AWS credential refresh thread")

    def stop_credential_refresh(self):
        """
        Method to stop the background credential refresh thread
        :return:
        """
        if self.__update_timer:
            self.__update_timer.cancel()
            blog = logger.BossLogger()
            blog.info("AWSManager - Stopped AWS credential refresh thread")

    def get_session(self):
        """
        Method to get a Boto3Session object.

        If session credentials have expired a new session is created.

        If no sessions are available (if a previous consumer of a session didn't return it for some reason, e.g. an
        exception occurred), a new session is created

        :return: bossutils.aws.Boto3Session
        """
        temp_session = None
        while True:
            try:
                temp_session = self.__sessions.get(block=False)

                # Check to make sure credentials haven't expired
                if temp_session.credential_id != self.__credential_set_id:
                    del temp_session
                    blog = logger.BossLogger()
                    blog.info("AWSManager - Session credentials expired. Creating a new session.")
                    self.__create_session()
                else:
                    break
            except queue.Empty:
                # No session was available so generate one
                blog = logger.BossLogger()
                blog.error("AWSManager - No session was available.  Dynamically created one!")
                self.__create_session()

        return temp_session

    def put_session(self, session):
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


# GLOBAL AWS MANAGER INSTANCE - Complex name for namespace safety
_AWS_MNGR_SDFSDNNFJBASFSGW = _aws_manager()


def get_aws_manager():
    """
    Generator function to access the global AWSManager instance. The instance will initialize on first access (will take
    ~5-15s to get AWS creds).  After initial load, the instance is readily available.

    :returns: The global AWSManager
    :rtype: bossutils.aws.AWSManager
    """
    aws_mngr = next(_AWS_MNGR_SDFSDNNFJBASFSGW)
    if aws_mngr:
        return aws_mngr
    else:
        return get_aws_manager()




