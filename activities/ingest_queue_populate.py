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
from bosscore.error import BossError, ErrorCodes, BossResourceNotFoundError

from heaviside.activities import fanout

log = logger.BossLogger().logger

POLL_DELAY = 0
STATUS_DELAY = 1
MAX_NUM_PROCESSES = 120
RAMPUP_DELAY = 15
RAMPUP_BACKOFF = 0.8
MAX_NUM_ITEMS_PER_LAMBDA = 45000


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
            'ingest_type': int (0 == TILE, 1 == VOLUMETRIC),
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
            'z_chunk_size': 16 for Tile or Probably 64 for Volumetric
        }

    Returns:
        {'arn': Upload queue ARN,
         'count': Number of messages put into the queue}
    """
    log.debug("Starting to populate upload queue")

    args['MAX_NUM_TILES_PER_LAMBDA'] = MAX_NUM_ITEMS_PER_LAMBDA

    if (args["ingest_type"] == 0) or (args["ingest_type"] == 1):
        raise BossError("{}".format("Unknown ingest_type: {}".format(args["ingest_type"])))

    clear_queue(args['upload_queue'])

    results = fanout(aws.get_session(),
                     args['upload_sfn'],
                     split_args(args),
                     max_concurrent=MAX_NUM_PROCESSES,
                     rampup_delay=RAMPUP_DELAY,
                     rampup_backoff=RAMPUP_BACKOFF,
                     poll_delay=POLL_DELAY,
                     status_delay=STATUS_DELAY)
    messages_uploaded = sum(results)

    if args["ingest_type"] == 0:
        tile_count = get_tile_count(args)
        if tile_count != messages_uploaded:
            log.warning("Messages uploaded do not match tile count.  tile count: {} messages uploaded: {}"
                      .format(tile_count, messages_uploaded))
        else:
            log.debug("tile count and messages uploaded match: {}".format(tile_count))

        return {
            'arn': args['upload_queue'],
            'count': tile_count,
        }
    elif args["ingest_type"] == 1:
        vol_count = get_volumetric_count(args)
        if vol_count != messages_uploaded:
            log.warning("Messages uploaded do not match volumetric count.  volumetric count: {} messages uploaded: {}"
                        .format(vol_count, messages_uploaded))
        else:
            log.debug("volumetric count and messages uploaded match: {}".format(vol_count))

        return {
            'arn': args['upload_queue'],
            'count': vol_count,
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

    tile_count = num_tiles_in('x') * num_tiles_in('y') * num_tiles_in('z') * num_tiles_in('t')
    return tile_count


def get_volumetric_count(args):
    tile_size = lambda v: args[v + "_tile_size"]
    extent = lambda v: args[v + '_stop'] - args[v + '_start']
    num_tiles_in = lambda v: math.ceil(extent(v) / tile_size(v))

    num_tiles_in_z = math.ceil(extent('z') / args['z_chunk_size'])

    tile_count = num_tiles_in('x') * num_tiles_in('y') * num_tiles_in_z * num_tiles_in('t')
    return tile_count


def split_args(args):
    if args["ingest_type"] == 0:
        # Compute # of tiles in the job
        count = get_tile_count(args)
        log.debug("Total Tile Count: " + str(count))
    else:
        # Compute # of volumetric chunks
        count = get_volumetric_count(args)
        log.debug("Total Chunk Count: " + str(count))
    offset_count = math.ceil(count / MAX_NUM_ITEMS_PER_LAMBDA)

    for item_count_offset in range(offset_count):
        args_ = args.copy()
        args_['items_to_skip'] = item_count_offset * MAX_NUM_ITEMS_PER_LAMBDA
        yield args_


def verify_count(args):
    """Verify that the number of messages in a queue is at least the given number

    Args:
        args: {
            'arn': ARN,
            'count': 0,
        }

    Returns:
        int: The total number of messages in the queue

    Raises:
        Error: If the messages in the queue is less then the count
    """

    session = aws.get_session()
    client = session.client('sqs')
    resp = client.get_queue_attributes(QueueUrl = args['arn'],
                                       AttributeNames = ['ApproximateNumberOfMessages'])
    messages = int(resp['Attributes']['ApproximateNumberOfMessages'])

    if messages > args['count']:
      log.debug("More SQS messages then expected: required tiles {}, actual messages {}".format(args['count'],
                                                                                                messages))
    elif messages < args['count']:
        raise Exception('Not enough messages in queue')

    return messages

