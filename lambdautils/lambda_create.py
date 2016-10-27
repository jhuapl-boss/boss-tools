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


from lambdautils import create_session

def create(args):
    if args.aws_credentials is None:
        # This allows aws roles to be used to create sessions.
        session = boto3.session.Session()
    else:
        session = create_session(args.aws_credentials)
    client = session.client('lambda')
    resp = client.create_function(
        FunctionName=args.name,
        Runtime='python2.7',
        Role=args.role,
        Handler=args.handler,
        Description=args.desc,
        Code={'S3Bucket': args.bucket, 'S3Key': args.key},
        Timeout=args.timeout,
        MemorySize=args.mem,
        VpcConfig={'SubnetIds': args.vpcsubnets, 'SecurityGroupIds': args.vpcsecgroups})
    print(resp)

def setup_parser():
    parser = argparse.ArgumentParser(
        description='Script for configuring lambda functions.  To supply arguments from a file, provide the filename prepended with an `@`.',
        fromfile_prefix_chars = '@')
    parser.add_argument(
        '--aws-credentials', '-a',
        metavar = '<file>',
        default = os.environ.get('AWS_CREDENTIALS'),
        type = argparse.FileType('r'),
        help = 'File with credentials for connecting to AWS (default: AWS_CREDENTIALS)')
    parser.add_argument(
        '--role', '-r',
        help = 'Role assigned to lambda function.')
    parser.add_argument(
        '--timeout', '-t',
        default = 3,
        type = int,
        help = 'Timeout in seconds.')
    parser.add_argument(
        '--mem', '-m',
        default = 128,
        type = int,
        help = 'Amount of memory in MB.  Must be a multiple of 64.')
    parser.add_argument(
        '--vpcsubnets', '-sn',
        default = [],
        help = 'List of subnet IDs in the lambda function\'s VPC.')
    parser.add_argument(
        '--vpcsecgroups', '-sg',
        default = [],
        help = 'List of security group IDs for the lambda function\'s VPC.')
    parser.add_argument(
        '--desc', '-d',
        metavar = 'DESCRIPTION',
        default = '',
        help = 'Lambda function description.'),
    parser.add_argument(
        'name',
        help = 'Name of function.')
    parser.add_argument(
        'key',
        help = 'S3 key that identifies zip containing lambda function.')
    parser.add_argument(
        'handler',
        help = 'Name of lambda handler function.')
    parser.add_argument(
        'bucket',
        help = 'Name of S3 bucket containing lambda function.')

    return parser


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()

    create(args)
