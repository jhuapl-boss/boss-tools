# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This lambda has been replaced by a step function.
#
from __future__ import print_function
from __future__ import absolute_import
from ndingest.settings.bosssettings import BossSettings

import urllib
import boto3
import copy
import json
import os
import sys
import tempfile
from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndingestproj.bossingestproj import BossIngestProj

MAX_BATCH_MSGS = 10

def download_from_s3(bucket, filename):
    """Download the given file from S3 and return the local file name.

    Args:
        bucket (S3.Bucket): S3 bucket containing messages for the SQS upload queue.
        filename (string): Name of file to download from the bucket.

    Returns:
        (string): Location in local file system of file downloaded from S3.
    """
    (fd, local_filename) = tempfile.mkstemp()
    with os.fdopen(fd, 'wb') as fp:
        bucket.download_fileobj(filename, fp)
    return local_filename


def enqueue_msgs(fp):
    """Parse given messages and send to SQS queue.

    Args:
        fp (file-like-object): File-like-object containing a header and messages.
    """
    read_header = False
    msgs = []
    upload_queue = None
    lineNum = 0

    for line in fp:
        lineNum += 1
        if not read_header:
            header = json.loads(line)
            if 'upload_queue_url' not in header:
                raise KeyError('Expected upload_queue_url in header')
            if 'ingest_queue_url' not in header:
                raise KeyError('Expected ingest_queue_url in header')
            if 'job_id' not in header:
                raise KeyError('Expected job_id in header')
            read_header = True
            continue

        try:
            msgs.append(parse_line(header, line))
        except:
            print('Error parsing line {}: {}'.format(lineNum, line))

        if len(msgs) == 1 and upload_queue is None:
            # Instantiate the upload queue object.
            asDict = json.loads(msgs[0])
            boss_ingest_proj = BossIngestProj.fromTileKey(asDict['tile_key'])
            boss_ingest_proj.job_id = header['job_id']
            upload_queue = UploadQueue(boss_ingest_proj)
        if len(msgs) >= MAX_BATCH_MSGS:
            # Enqueue messages.
            upload_queue.sendBatchMessages(msgs)
            msgs = []

    if len(msgs) > 0:
        # Final enqueue messages of remaining messages.
        upload_queue.sendBatchMessages(msgs)


def parse_line(header, line):
    """Parse one line of data from the message file.

    Each line is expected to contain chunk key - comma - tile key (CSV style).

    Args:
        header (dict): Data to join with contents of line to construct a full message.
        line (string): Contents of the line.

    Returns:
        (string): JSON encoded data ready for enqueuing.

    Raises:
        (RuntimeError): if less than 2 columns found on a line.
    """
    msg = {}
    msg['job_id'] = header['job_id']
    msg['upload_queue_arn'] = header['upload_queue_url']
    msg['ingest_queue_arn'] = header['ingest_queue_url']

    tokens = line.split(',')
    if len(tokens) < 2:
        raise RuntimeError('Bad message line encountered.')

    msg['chunk_key'] = tokens[0].strip()
    msg['tile_key'] = tokens[1].strip()

    return json.dumps(msg)


if __name__ == '__main__':
    # Load settings
    SETTINGS = BossSettings.load()

    # Parse input args passed as a JSON string from the lambda loader
    json_event = sys.argv[1]
    event = json.loads(json_event)
    print(event)

    # Extract bucket and filename.
    bucket_name = event['upload_bucket_name']
    filename = event['filename']

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Download file.
    local_filename = download_from_s3(bucket, filename)

    # Parse and start enqueuing.
    with open(local_filename) as fp:
        enqueue_msgs(fp)

    # Clean up.
    os.remove(local_filename)
    bucket.delete_objects(Delete={
        'Objects': [{ 'Key': filename }],
        'Quiet': True
    })
