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

from __future__ import print_function
from __future__ import absolute_import
from ndingest.settings.bosssettings import BossSettings

import urllib
import boto3
import copy
import json
import os
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
    for line in fp:
        if not read_header:
            header = json.loads(line)
            read_header = True
            continue

        msgs.append(parse_line(header, line))

        if len(msgs) == 1 and upload_queue is None:
            # Instantiate the upload queue object.
            asDict = json.loads(msgs[0])
            boss_ingest_proj = BossIngestProj.fromTileKey(asDict['tile_key'])
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

    Args:
        header (dict): Data to join with contents of line to construct a full message.
        line (string): Contents of the line.

    Returns:
        (string): JSON encoded data ready for enqueuing.
    """
    working_header = copy.deepcopy(header)
    # TODO: Extract chunk key and tile key from line.

    return json.dumps(working_header)

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
    enqueue_msgs(local_filename)
