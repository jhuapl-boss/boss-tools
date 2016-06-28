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
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import boto3
import os
import sys

from bossutils.deploy_lambdas import S3_BUCKET
from bossutils.deploy_lambdas import create_session

def update_code(args):
    """Point the lambda function at new code.
    """
    session = create_session(args.aws_credentials)
    client = session.client('lambda')
    resp = client.update_function_code(
        FunctionName=args.name,
        S3Bucket = args.bucket,
        S3Key = args.key)
    print(resp)

def setup_parser():
    parser = argparse.ArgumentParser(
        description='Script for updating lambda function code.  To supply arguments from a file, provide the filename prepended with an `@`.',
        fromfile_prefix_chars = '@')
    parser.add_argument(
        '--aws-credentials', '-a',
        metavar = '<file>',
        default = os.environ.get('AWS_CREDENTIALS'),
        type = argparse.FileType('r'),
        help = 'File with credentials for connecting to AWS (default: AWS_CREDENTIALS)')
    parser.add_argument(
        '--bucket', '-b',
        default = S3_BUCKET,
        help = 'Name of S3 bucket containing lambda function.')
    parser.add_argument(
        'name',
        help = 'Name of function.')
    parser.add_argument(
        'key',
        help = 'S3 key that identifies zip containing lambda function.')

    return parser


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()

    if args.aws_credentials is None:
        parser.print_usage()
        print("Error: AWS credentials not provided and AWS_CREDENTIALS is not defined.")
        sys.exit(1)

    update_code(args)
