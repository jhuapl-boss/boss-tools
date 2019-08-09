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
import subprocess
import sys
import tempfile

def zip(src_folder, zip_name):
    """Zip all the files and folders in src_folder.

    Note that the zip command is run _within_ src_folder so the folder, 
    itself, will not appear in the zip.

    Args:
        src_folder (string): Folder to zip.
        zip_name (string): Name of output zip file.

    Returns:
        (bool): True on success.
    """

    #args = ('/usr/bin/zip', '--symlinks', '-r', '-q', zip_name, '.')
    args = ('/usr/bin/zip', '-r', '-q', zip_name, '.')
    popen = subprocess.Popen(args, cwd=src_folder, stdout=subprocess.PIPE)
    exit_code = popen.wait()
    output = popen.stdout.read()
    if not exit_code == 0:
        print(str(output))
        return False
    return True


def upload_to_s3(session, zip_file, bucket):
    """Upload the zip file to the given S3 bucket.

    Args:
        session (Session): Boto3 Session.
        zip_file (string): Name of zip file.  The name (after any path is stripped) is used as the key.
        bucket (string): Name of bucket to use.
    """
    key = os.path.basename(zip_file)
    s3 = session.client('s3')
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    s3.put_object(Bucket=bucket, Key=key, Body=open(zip_file, 'rb'))


def setup_parser():
    parser = argparse.ArgumentParser(
        description='Script for deploying lambda functions to S3.')
    parser.add_argument(
        '--aws-credentials', '-a',
        metavar = '<file>',
        default = os.environ.get('AWS_CREDENTIALS'),
        type = argparse.FileType('r'),
        help = 'File with credentials for connecting to AWS (default: AWS_CREDENTIALS)')
    parser.add_argument(
        '--upload-only', '-u',
        action = 'store_const',
        const = True,
        default = False,
        help = 'Don\'t re-create the zip.  Just upload to S3.')
    parser.add_argument(
        'source_folder',
        help = 'Folder containing lambda handlers and the virtualenv.')
    parser.add_argument(
        'zip_file',
        help = 'source_folder will be zipped and stored in this filename.')
    parser.add_argument(
        'bucket',
        help = 'Name of S3 bucket to upload lambdas to.')

    return parser


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()

    cwd = os.getcwd()

    # Build an absolute path to the zip file if one not provided.
    zip_file_abs_path = os.path.join(cwd, args.zip_file)

    if not args.upload_only:
        if not zip(args.source_folder, zip_file_abs_path):
            sys.exit(1)

    if args.aws_credentials is None:
        # This allows aws roles to be used to create sessions.
        session = boto3.session.Session()
    else:
        # moved so the script can be independent if not passing credentials
        from lambdautils import create_session
        session = create_session(args.aws_credentials)
    upload_to_s3(session, args.zip_file, args.bucket)

    print('Done.\n')

