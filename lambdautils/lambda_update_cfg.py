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

from lambdautils import S3_BUCKET
from lambdautils import create_session

def update_cfg(args):
    """Update the configuration (everything but code) of the lambda function.
    """
    session = create_session(args.aws_credentials)
    client = session.client('lambda')

    kwargs = { 'FunctionName': args.name, 'Runtime': 'python2.7' }
    if args.role is not None:
        kwargs['Role'] = args.role

    if args.handler is not None:
        kwargs['Handler'] = args.handler

    if args.desc is not None:
        kwargs['Description'] = args.desc

    if args.timeout is not None:
        kwargs['Timeout'] = args.timeout

    if args.mem is not None:
        kwargs['MemorySize'] = args.mem

    if args.vpcsubnets is not None and args.vpcsecgroups is not None:
        kwargs['VpcConfig'] = {
            'SubnetIds': args.vpcsubnets, 
            'SecurityGroupIds': args.vpcsecgroups
        }

    resp = client.update_function_configuration(**kwargs)
    print(resp)

def setup_parser():
    parser = argparse.ArgumentParser(
        description='Script for updating lambda functions.  Note, code cannot be updated via this script.  Use lamda_update_code.py for that.  To supply arguments from a file, provide the filename prepended with an `@`.',
        fromfile_prefix_chars = '@')
    parser.add_argument(
        '--aws-credentials', '-a',
        metavar = '<file>',
        default = os.environ.get('AWS_CREDENTIALS'),
        type = argparse.FileType('r'),
        help = 'File with credentials for connecting to AWS (default: AWS_CREDENTIALS)')
    parser.add_argument(
        '--role', '-r',
        default = None,
        help = 'Role assigned to lambda function.')
    parser.add_argument(
        '--timeout', '-t',
        default = None,
        type = int,
        help = 'Timeout in seconds.')
    parser.add_argument(
        '--mem', '-m',
        default = None,
        type = int,
        help = 'Amount of memory in MB.  Must be a multiple of 64.')
    parser.add_argument(
        '--vpcsubnets', '-sn',
        default = None,
        help = 'List of subnet IDs in the lambda function\'s VPC.')
    parser.add_argument(
        '--vpcsecgroups', '-sg',
        default = None,
        help = 'List of security group IDs for the lambda function\'s VPC.')
    parser.add_argument(
        '--desc', '-d',
        metavar = 'DESCRIPTION',
        default = None,
        help = 'Lambda function description.'),
    parser.add_argument(
        '--handler', '-hnd',
        default = None,
        help = 'Name of lambda handler function.')
    parser.add_argument(
        'name',
        help = 'Name of function.')

    return parser


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()

    if args.aws_credentials is None:
        parser.print_usage()
        print("Error: AWS credentials not provided and AWS_CREDENTIALS is not defined.")
        sys.exit(1)

    if ((args.vpcsecgroups is not None and args.vpcsubnets is None)
        or (args.vpcsecgroups is None and args.vpcsubnets is not None)):
        print('Error: either specify both vpcsecgroups and vpcsubnets or do not specify either.')

    update_cfg(args)
