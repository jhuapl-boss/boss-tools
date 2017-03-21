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

import json
import time
from datetime import datetime

from bossutils import aws
from bossutils import logger

log = logger.BossLogger().logger

class UnknownSubprocess(Exception):
    pass

class FailedToSendMessages(Exception):
    pass

class SFN(object):
    class Status(object):
        __slots__ = ('resp')

        def __init__(self, resp):
            self.resp = resp

        @property
        def failed(self):
            return self.resp['status'] == 'FAILED'

        @property
        def success(self):
            return self.resp['status'] == 'SUCCEEDED'

        @property
        def output(self):
            return json.loads(self.resp['output']) if 'output' in self.resp else None

    def __init__(self, name):
        session = aws.get_session()
        self.client = session.client('stepfunctions')
        self.arn = None

        resp = self.client.list_state_machines()
        for machine in resp['stateMachines']:
            arn = machine['stateMachineArn']
            if arn.endswith(name):
                self.arn = arn
                break

        if self.arn is None:
            raise UnknownSubprocess("Could not find stepfunction {}".format(name))

    def launch(self, args):
        resp = self.client.start_execution(stateMachineArn = self.arn,
                                           name = datetime.now().strftime("%Y%m%d%H%M%s%f"),
                                           input = json.dumps(args))
        return resp['executionArn']

    def status(self, arn):
        resp = self.client.describe_execution(executionArn = arn)
        return SFN.Status(resp)

POLL_DELAY = 5
MAX_NUM_PROCESSES = 100
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

    sfn = SFN(args['upload_sfn'])

    sub_args = split_args(args)
    arns = []
    delay = RAMPUP_DELAY
    started_all = False
    total_sent = 0
    while True:
        try:
            while not started_all and len(arns) < MAX_NUM_PROCESSES:
                arns.append(sfn.launch(next(sub_args)))
                time.sleep(delay)
                if delay > 0:
                    delay = int(delay * RAMPUP_BACKOFF)
        except StopIteration:
            started_all = True
            log.debug("Finished launching sub-processes")

        time.sleep(POLL_DELAY)

        arns_ = []
        for arn in arns:
            status = sfn.status(arn)
            if status.failed:
                log.debug("Sub-process failed, restarting")
                raise FailedToSendMessages()
            elif status.success:
                total_sent += status.output
            else:
                arns_.append(arn)
        log.debug("Sub-processes finished: {}".format(len(arns) - len(arns_)))
        log.debug("Sub-processes running: {}".format(len(arns_)))
        arns = arns_

        if started_all and len(arns) == 0:
            log.debug("Finished")
            break

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

