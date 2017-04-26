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
            'z_tile_size': 16,
        }

    Returns:
        {'arn': Upload queue ARN,
         'count': Number of messages put into the queue}
    """
    log.debug("Starting to populate upload queue")

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

def split_args(args):
    range_ = lambda v: range(args[v + '_start'], args[v + '_stop'], args[v + '_tile_size'])

    for t in range_('t'):
        for z in range_('z'):
            args_ = args.copy()

            args_['t_start'] = t
            args_['t_stop'] = t + args['t_tile_size']

            args_['z_start'] = z
            args_['z_stop'] = z + args['z_tile_size']
            args_['final_z_stop'] = args['z_stop']

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

