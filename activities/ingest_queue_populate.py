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

import time
import math
from functools import reduce

from bossutils import aws
from bossutils import logger

from heaviside.activities import fanout

log = logger.BossLogger().logger

POLL_DELAY = 5
STATUS_DELAY = 1
MAX_NUM_PROCESSES = 50
RAMPUP_DELAY = 15
RAMPUP_BACKOFF = 0.8
MAX_NUM_TILES_PER_LAMBDA = 30000  # this has to match boss-manage/cloud_formation/lambda/ingest_populate/ingest_queue_upload.py

def ingest_populate(args):
    """Populate the ingest upload SQS Queue with tile information

    Note: This activity will clear the upload queue of any existing
          messages

    Args:
        args: {
            'upload_sfn': ARN,

            'job_id': '',
            'upload_queue': ARN,
            'ingest_queue': ARN,

            'resolution': 0,
            'project_info': [col_id, exp_id, ch_id],

            't_start': 0,
            't_stop': 0,
            't_tile_size': 0,

            'x_start': 0,
            'x_stop': 0,
            'x_tile_size': 0,

            'y_start': 0,
            'y_stop': 0
            'y_tile_size': 0,

            'z_start': 0,
            'z_stop': 0
            'z_tile_size': 1,
        }

    Returns:
        {'arn': Upload queue ARN,
         'count': Number of messages put into the queue}
    """
    log.debug("Starting to populate upload queue")
    log.debug("Sandy's new code")

    args['MAX_NUM_TILES_PER_LAMBDA'] = MAX_NUM_TILES_PER_LAMBDA
    args['z_chunk_size'] = 16
    args['z_tile_size'] = 1

    clear_queue(args['upload_queue'])

    results = fanout(aws.get_session(),
                     args['upload_sfn'],
                     split_args(args),
                     max_concurrent = MAX_NUM_PROCESSES,
                     rampup_delay = RAMPUP_DELAY,
                     rampup_backoff = RAMPUP_BACKOFF,
                     poll_delay = POLL_DELAY,
                     status_delay = STATUS_DELAY)

    total_sent = reduce(lambda x, y: x+y, results, 0)

    return {
        'arn': args['upload_queue'],
        'count': total_sent,
    }

def clear_queue(arn):
    """Delete any existing messages in the given SQS queue

    Args:
        arn (string): SQS ARN of the queue to empty
    """
    log.debug("Clearing queue {}".format(arn))
    session = aws.get_session()
    client = session.client('sqs')
    client.purge_queue(QueueUrl = arn)
    time.sleep(60)


def get_tile_count(args):
    tile_size = lambda v: args[v + "_tile_size"]
    extent = lambda v: args[v + '_stop'] - args[v + '_start']
    num_tiles_in = lambda v: math.ceil(extent(v) / tile_size(v))

    x_extent = args['x_stop'] - args['x_start']
    y_extent = args['y_stop'] - args['y_start']
    z_extent = args['z_stop'] - args['z_start']
    t_extent = args['t_stop'] - args['t_start']
    num_tiles_in_x = math.ceil(x_extent / args['x_tile_size'])
    num_tiles_in_y = math.ceil(y_extent / args['y_tile_size'])
    num_tiles_in_z = math.ceil(z_extent / args['z_tile_size'])
    num_tiles_in_t = math.ceil(t_extent / args['t_tile_size'])
    tile_count = num_tiles_in_x * num_tiles_in_y * num_tiles_in_z * num_tiles_in_t

    # We add the multiple by 16 in the tile_count because the actual z tile_size is 1, not 16.  The z_tile_size of 16 is needed in the lambda code to loop over the range correctly.
    # tile_count = num_tiles_in('x') * num_tiles_in('y') * num_tiles_in('z') * 16 * num_tiles_in('t')
    return tile_count


def split_args(args):
    # Compute # of tiles in the job
    tile_count = get_tile_count(args)
    log.debug("Total Tile Count: " + str(tile_count))

    offset_count = math.ceil(tile_count / MAX_NUM_TILES_PER_LAMBDA)

    for tiles_count_offset in range(offset_count):
        args_ = args.copy()
        args_['tiles_to_skip'] = tiles_count_offset * MAX_NUM_TILES_PER_LAMBDA
        yield args_

def verify_count(args):
    """Verify that the number of messages in a queue is the given number

    Args:
        args: {
            'arn': ARN,
            'count': 0,
        }

    Returns:
        int: The total number of messages in the queue

    Raises:
        Error: If the count doesn't match the messages in the queue
    """

    session = aws.get_session()
    client = session.client('sqs')
    resp = client.get_queue_attributes(QueueUrl = args['arn'],
                                       AttributeNames = ['ApproximateNumberOfMessages'])
    messages = int(resp['Attributes']['ApproximateNumberOfMessages'])

    if messages != args['count']:
        raise Exception('Counts do not match')

    return args['count']

