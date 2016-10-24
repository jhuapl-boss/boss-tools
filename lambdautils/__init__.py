#!/usr/bin/env python3

# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import boto3
import json

S3_BUCKET = 'boss-lambda-env'

def create_session(cred_fh):
    """Read AWS credentials from the given file object and create a Boto3 session.

        Note: Currently is hardcoded to connect to Region US-East-1

    Args:
        cred_fh (file): File object of a JSON formated data with the following keys "aws_access_key" and "aws_secret_key".

    Returns:
        (Session): Boto3 session.
    """
    credentials = json.load(cred_fh)

    session = boto3.Session(
        aws_access_key_id = credentials["aws_access_key"],
        aws_secret_access_key = credentials["aws_secret_key"],
        region_name = 'us-east-1')
    return session
