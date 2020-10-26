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

"""
The activities in this file manage a downsample.
See boss-manage.git/cloud_formation/stepfunctions/resolution_hierarchy.hsd
as well as boss-manage.git/docs/downsample.

Note that there are several SQS queues used during downsample.  There is the
downsample job queue which holds independent downsample jobs waiting for
processing.  This queue is "permanent".

Other SQS queues are created while running a downsample job.  These hold
cuboids waiting for downsampling.
"""

from bossutils import aws, logger
from spdb.c_lib.ndtype import CUBOIDSIZE

from bossutils.multidimensional import XYZ, ceildiv
from bossutils.multidimensional import range as xyz_range

import types
import json
import time
import random
from multiprocessing import Pool, cpu_count
from datetime import timedelta, datetime
import pymysql.cursors
from boss_db import get_db_connection

log = logger.bossLogger()

class DownsampleStatus:
    """
    String values are actual values stored in DB.

    Taken from boss.git/django/bosscore/models.py

    ToDo: consider making a separate shared repo with constants that the
    Django repo, step functions, and lambdas can all import.
    """
    NOT_DOWNSAMPLED = 'NOT_DOWNSAMPLED'
    IN_PROGRESS = 'IN_PROGRESS'
    DOWNSAMPLED = 'DOWNSAMPLED'
    FAILED = 'FAILED'
    QUEUED = 'QUEUED'

######################################################
### Configure Remote Debugging via SIGUSR1 trigger ###
def sigusr_handler(sig, frame):
    try:
        import rpdb2
        rpdb2.start_embedded_debugger('password')
    except:
        log.exception("SIGUSR1 caught but embedded debugger could not be started")

def enable_debug_handler():
    import signal
    signal.signal(signal.SIGUSR1, sigusr_handler)
######################################################


###########################################################
###               Configuration Options                 ###
### Modify these values to modify the activity behavior ###

# int: The number of volumes each downsample_volume lambda should downsample
BUCKET_SIZE = 8

# datetime.delta: The runtime of the downsample_volume lambda
MAX_LAMBDA_TIME = timedelta(seconds=120)

# Periodically update downsample job's SQS visibility timeout so it doesn't
# get redelivered.
NEW_VISIBILITY_TIMEOUT = MAX_LAMBDA_TIME * 2

# int: The number of status poll cycles that have the same count for more
#      lambdas to be launched (total time is UNCHANGING_LAUNCH * MAX_LAMBDA_TIME)
UNCHANGING_LAUNCH = 3

# int: The number of status poll cycles that have the same count for the
#      activity to check the lambda throttle count and wait until throttling
#      is no longer effecting the cubes queue count
UNCHANGING_THROTTLE = 4

# int: The number of status poll cycles that have to be the same for an
#      exception to be raised
#      This is a fail safe so the activity doesn't hang if there is a problem
UNCHANGING_MAX = 5

# int: The number of multiprocessing pool workers to use
POOL_SIZE = int(cpu_count() * 2)

# int: The number of status poll cycles that have a count of 0 (zero) before the
#      polling loop exits
ZERO_COUNT = 3

# int: The number of extra lambdas to fire off each round
EXTRA_LAMBDAS = 10

###########################################################

class ResolutionHierarchyError(Exception):
    pass

class LambdaLaunchError(ResolutionHierarchyError):
    pass

class FailedLambdaError(ResolutionHierarchyError):
    def __init__(self):
        msg = "A Lambda failed execution"
        super().__init__(msg)

def check_downsample_queue(args):
    """
    Check queue for downsample jobs.

    Also marks downsample as in progress if message found.

    Args:
        (dict): { 'queue_url': <URL of SQS queue>, 'sfn_arn': <arn of the downsample step fcn> }

    Returns:
        (dict): {
            'start_downsample': True | False,
            'queue_url': <URL of SQS queue>,
            'sfn_arn': <arn of the downsample step fcn>,
            'status': 'IN_PROGRESS',
            ... }
            if start_downsample, then args for downsample_channel() provided including
            job_receipt_handle so message's visibility timeout can be
            adjusted or be deleted from the queue.
    """
    session = aws.get_session()
    sqs = session.client('sqs')
    resp = sqs.receive_message(QueueUrl=args['queue_url'], WaitTimeSeconds=2, MaxNumberOfMessages=1)
    if 'Messages' not in resp or len(resp['Messages']) == 0:
        return { 'start_downsample': False }

    msg = resp['Messages'][0]
    job = json.loads(msg['Body'])
    job['start_downsample'] = True
    job['job_receipt_handle'] = msg['ReceiptHandle']
    job['queue_url'] = args['queue_url']
    job['sfn_arn'] = args['sfn_arn']
    job['status'] = DownsampleStatus.IN_PROGRESS
    return job

def delete_downsample_job(args):
    """
    Delete the message for the finished downsample job from SQS.

    Returns a dict that allows the last state to start a new instance of the
    downsample step function.

    Args:
        args (dict): { 'queue_url': <URL of SQS queue>, 'job_receipt_handle': <msg's receipt handle, ... }

    Returns:
        (dict): { 'queue_url': <URL of SQS queue>, 'sfn_arn': <arn of the downsample step fcn>, 'db_host': MySQL host name, 'status': 'DOWNSAMPLED' }
    """
    try:
        session = aws.get_session()
        sqs = session.client('sqs')
        sqs.delete_message(QueueUrl=args['queue_url'], ReceiptHandle=args['job_receipt_handle'])
        return {
            'sfn_arn': args['sfn_arn'],
            'queue_url': args['queue_url'],
            'db_host': args['db_host'],
            'status': DownsampleStatus.DOWNSAMPLED,
        }
    except Exception as ex:
        log.exception(f'Error trying to downsample job from SQS: {ex}')
        raise

def update_visibility_timeout(queue_url, receipt_handle):
    """
    Update the visibility timeout of the message for the current downsample job.

    This keeps the message from getting redelivered while the downsample is
    still running.

    Args:
        queue_url (str): URL of SQS queue.
        receipt_handle (str): Message's receipt handle.

    Returns:

    Raises:
    """
    try:
        session = aws.get_session()
        sqs = session.client('sqs')
        sqs.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=NEW_VISIBILITY_TIMEOUT.seconds)
    except Exception as ex:
        log.exception(f'Error trying to update visibilty timeout of downsample job: {ex}')
        raise

def update_downsample_status_in_db(args):
    """
    Update the downsample status in the MySQL database.

    Args:
        args (dict):
            db_host (str): MySQL host name.
            channel_id (int): ID of channel for downsample.
            status (str): String from DownsampleStatus class.

    Returns:
        (dict): Returns input args for passing to next SFN state.
    """
    sql = """
        UPDATE channel
        SET status = %(status)s
        WHERE id = %(chan_id)s
        """

    db_host = args['db_host']
    chan_id = args['channel_id']
    status = args['status']

    sql_args = dict(status=status, chan_id=str(chan_id))

    try:
        db_connection = get_db_connection(self.db_host)
        with db_connection.cursor(pymysql.cursors.SSCursor) as cursor:
            rows = cursor.execute(sql, sql_args)
            if rows < 1:
                log.error(
                    f'DB said no rows updated when trying to set downsample status to {status} for channel {chan_id}'
                )
    except Exception as ex:
        log.exception(f'Failed to set downsample status to {status} for channel {chan_id}: {ex}')

    return args

def downsample_channel(args):
    """
    Slice the given channel into chunks of 2x2x2 or 2x2x1 cubes that are then
    sent to the downsample_volume lambda for downsampling into a 1x1x1 cube at
    resolution + 1.

    Makes use of the bossutils.multidimensional library for simplified vector
    math.

    Generators are used as much as possible (instead of lists) so that large
    lists of data are not actualized and kept in memory.

    Args:
        args {
            downsample_volume_lambda (ARN | lambda name)

            collection_id (int)
            experiment_id (int)
            channel_id (int)
            annotation_channel (bool)
            data_type (str) 'uint8' | 'uint16' | 'uint64'

            s3_bucket (URL)
            s3_index (URL)

            x_start (int)
            y_start (int)
            z_start (int)

            x_stop (int)
            y_stop (int)
            z_stop (int)

            resolution (int) The resolution to downsample. Creates resolution + 1
            resolution_max (int) The maximum resolution to generate
            res_lt_max (bool) = args['resolution'] < (args['resolution_max'] - 1)

            type (str) 'isotropic' | 'anisotropic'
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both

            aws_region (str) AWS region to run in such as us-east-1

            start_downsample (bool) Used by previous state to indicate that a downsample job exists
            job_receipt_handle (str) Used by downstream state to delete the downsample job from queue
            queue_url (str) URL of downsample queue; downstream state deletes from this queue
            sfn_arn (str) <arn of the downsample step fcn>
            db_host (str) Host of MySQL database.
        }

    Return:
        dict: An updated argument dictionary containing the shrunk frame,
              resolution, and res_lt_max values
    """

    # TODO: load downsample_volume_lambda from boss config

    #log.debug("Downsampling resolution " + str(args['resolution']))

    resolution = args['resolution']

    dim = XYZ(*CUBOIDSIZE[resolution])
    #log.debug("Cube dimensions: {}".format(dim))

    def frame(key):
        return XYZ(args[key.format('x')], args[key.format('y')], args[key.format('z')])

    # Figure out variables for isotropic, anisotropic, or isotropic and anisotropic
    # downsampling. If both are happening, fanout one and then the other in series.
    configs = []
    if args['type'] == 'isotropic':
        configs.append({
            'name': 'isotropic',
            'step': XYZ(2,2,2),
            'iso_flag': False,
            'frame_start_key': '{}_start',
            'frame_stop_key': '{}_stop',
        })
    else:
        configs.append({
            'name': 'anisotropic',
            'step': XYZ(2,2,1),
            'iso_flag': False,
            'frame_start_key': '{}_start',
            'frame_stop_key': '{}_stop',
        })

        # if this iteration will split into aniso and iso downsampling, copy the coordinate frame
        if resolution == args['iso_resolution']:
            def copy(var):
                args['iso_{}_start'.format(var)] = args['{}_start'.format(var)]
                args['iso_{}_stop'.format(var)] = args['{}_stop'.format(var)]
            copy('x')
            copy('y')
            copy('z')

        if resolution >= args['iso_resolution']: # DP TODO: Figure out how to launch aniso iso version with mutating arguments
            configs.append({
                'name': 'isotropic',
                'step': XYZ(2,2,2),
                'iso_flag': True,
                'frame_start_key': 'iso_{}_start',
                'frame_stop_key': 'iso_{}_stop',
            })

    for config in configs:
        # Different ID and queue for each resolution, as it takes 60 seconds to delete a queue
        # Different ID and queue for each iso/aniso downsample incase a a DLQ message is received
        #     for the previous config
        downsample_id = str(random.random())[2:] # remove the '0.' part of the number
        dlq_arn = create_queue('downsample-dlq-' + downsample_id)
        cubes_arn = create_queue('downsample-cubes-' + downsample_id)

        try:
            frame_start = frame(config['frame_start_key'])
            frame_stop = frame(config['frame_stop_key'])
            step = config['step']
            use_iso_flag = config['iso_flag'] # If the resulting cube should be marked with the ISO flag

            # Round to the furthest full cube from the center of the data
            cubes_start = frame_start // dim
            cubes_stop = ceildiv(frame_stop, dim)

            # For a non-zero start, make sure start cube aligns with a zero start downsample
            # so that the data aligns and there are no shifts with the new downsampled data
            mod = cubes_start % step
            if mod.x != 0:
                cubes_start = XYZ(cubes_start.x - 1, cubes_start.y, cubes_start.z)
            if mod.y != 0:
                cubes_start = XYZ(cubes_start.x, cubes_start.y - 1, cubes_start.z)
            if mod.z != 0:
                cubes_start = XYZ(cubes_start.x, cubes_start.y, cubes_start.z - 1)

            log.debug('Downsampling {} resolution {}'.format(config['name'], resolution))
            log.debug("Frame corner: {}".format(frame_start))
            log.debug("Frame extent: {}".format(frame_stop))
            log.debug("Cubes corner: {}".format(cubes_start))
            log.debug("Cubes extent: {}".format(cubes_stop))
            log.debug("Downsample step: {}".format(step))

            log.debug("Populating input cube")
            cube_count = populate_cubes(cubes_arn, cubes_start, cubes_stop, step)

            log.debug("Invoking downsample lambdas")
            lambda_count = ceildiv(cube_count, BUCKET_SIZE) + EXTRA_LAMBDAS
            lambda_args = {
                'bucket_size': BUCKET_SIZE,
                'args': args,
                'step': step,
                'dim': dim,
                'use_iso_flag': use_iso_flag,
                'dlq_arn': dlq_arn,
                'cubes_arn': cubes_arn,
            }

            launch_lambdas(lambda_count,
                           args['downsample_volume_lambda'],
                           json.dumps(lambda_args).encode('UTF8'),
                           dlq_arn,
                           cubes_arn,
                           args['queue_url'],
                           args['job_receipt_handle'])

            # Resize the coordinate frame extents as the data shrinks
            # DP NOTE: doesn't currently work correctly with non-zero frame starts
            def resize(var, size):
                start = config['frame_start_key'].format(var)
                stop = config['frame_stop_key'].format(var)
                args[start] //= size
                args[stop] = ceildiv(args[stop], size)
            resize('x', step.x)
            resize('y', step.y)
            resize('z', step.z)
        finally:
            delete_queue(dlq_arn)
            delete_queue(cubes_arn)

    # Advance the loop and recalculate the conditional
    # Using max - 1 because resolution_max should not be a valid resolution
    # and res < res_max will end with res = res_max - 1, which generates res_max resolution
    args['resolution'] = resolution + 1
    args['res_lt_max'] = args['resolution'] < (args['resolution_max'] - 1)
    return args

def chunk(xs, size):
    """Chunk the input iterable into lists of the requested size

    Args:
        xs (iterable): A list or generator of items to chunk
        size (int): The number of items to include in each output lists

    Return:
        Generator: A Generator of lists, each of length size
    """
    ys = []
    for x in xs:
        ys.append(x)
        if len(ys) == size:
            yield ys
            ys = []
    if len(ys) > 0:
        yield ys

def num_cubes(start, stop, step):
    """Calculate the number of volumes to be downsamples

    Used so all of the results from make_cubes() doesn't have to be pulled into
    memory.

    Args:
        start (XYZ): Starting corner of the coordinate frame
        stop (XYZ): Far corner of the coordinate frame
        step (XYZ): The size of each volume to downsample

    Return:
        int: The number of volumes in the frame
    """
    extents = ceildiv(stop - start, step)
    return int(extents.x * extents.y * extents.z)

def make_cubes(start, stop, step):
    """Produce the target cubes to downsample

    Args:
        start (XYZ): Starting corner of the coordinate frame
        stop (XYZ): Far corner of the coordinate frame
        step (XYZ): The size of each volume to downsample

    Return:
        Generator: A Generator of XYZ target cubes
    """
    for target in xyz_range(start, stop, step = step):
        yield target # XYZ type is automatically handled by JSON.dumps
                     # Since it is a subclass of tuple

def populate_cubes(queue_arn, start, stop, step):
    """Populate the given SQS queue with the target cubes to downsample

    Args:
        queue_arn (str): The target SQS queue URL
        start (XYZ): Starting corner of the coordinate frame
        stop (XYZ): Far corner of the coordinate frame
        step (XYZ): The size of each volume to downsample

    Return:
        int: The number of cubes enqueued
    """
    # evenly chunk cubes into POOL_SIZE lists
    count = num_cubes(start, stop, step)
    enqueue_size = ceildiv(count, POOL_SIZE)

    args = ((queue_arn, cubes)
            for cubes in chunk(make_cubes(start, stop, step), enqueue_size))

    log.debug("Enqueueing {} cubes in chunks of {} using {} processes".format(count, enqueue_size, POOL_SIZE))

    start = datetime.now()
    with Pool(POOL_SIZE) as pool:
        pool.starmap(enqueue_cubes, args)
    stop = datetime.now()
    log.info("Enqueued {} cubes in {}".format(count, stop - start))

    return count

def enqueue_cubes(queue_arn, cubes):
    """Multiprocessing.Pool worker function for enqueuing a number of messages

    Called by populate_cubes()

    Args:
        queue_arn (str): The target SQS queue URL
        cubes (list[XYZ]): A list of XYZ cubes to enqueue
    """
    try:
        sqs = aws.get_session().resource('sqs')
        queue = sqs.Queue(queue_arn)
        count = 0

        msgs = ({'Id': str(id(cube)),
                 'MessageBody': json.dumps(cube)}
                for cube in cubes)

        for batch in chunk(msgs, 10): # 10 is the message batch limit for SQS
            count += 1
            if count % 500 == 0:
                log.debug ("Enqueued {} cubes".format(count * 10))

            queue.send_messages(Entries=batch)

    except Exception as ex:
        log.exception("Error caught in process, raising to controller")
        raise ResolutionHierarchyError(str(ex))

def launch_lambdas(total_count, lambda_arn, lambda_args, dlq_arn, cubes_arn, downsample_queue_url, receipt_handle):
    """Launch lambdas to process all of the target cubes to downsample

    Launches an initial set of lambdas and monitors the cubes SQS queue to
    understand the current status. If the count in the queue doesn't change
    for UNCHANGING_LAUNCH cycles then it will calculate how many more lambdas
    to launch and launch them.

    If the queue count doesn't change after launching more lambdas an exception
    will eventually be raised so the activity is not hanging forever.

    Args:
        total_count (int): The initial number of lambdas to launch
        lambda_arn (str): Name or ARN of the lambda function to invoke
        lambda_args (str): The lambda payload to pass when invoking
        dlq_arn (str): ARN of the SQS DLQ to monitor for error messages
        cubes_arn (str): ARN of the input cubes SQS queue to monitor for
                         completion of the downsample
        downsample_queue_url (str): URL of downsample job queue
        receipt_handle (str): Handle of message from downsample queue
    """
    per_lambda = ceildiv(total_count, POOL_SIZE)
    d,m = divmod(total_count, per_lambda)
    counts = [per_lambda] * d
    if m > 0:
        counts += [m]

    assert sum(counts) == total_count, "Didn't calculate counts per lambda correctly"

    log.debug("Launching {} lambdas in chunks of {} using {} processes".format(total_count, per_lambda, POOL_SIZE))

    args = ((count, lambda_arn, lambda_args, dlq_arn)
            for count in counts)

    start = datetime.now()
    with Pool(POOL_SIZE) as pool:
        pool.starmap(invoke_lambdas, args)
    stop = datetime.now()
    log.info("Launched {} lambdas in {}".format(total_count, stop - start))

    # Finished launching lambdas, need to wait for all to finish
    log.info("Finished launching lambdas")

    polling_start = datetime.now()
    previous_count = 0
    count_count = 1
    zero_count = 0
    while True:
        if check_queue(dlq_arn) > 0:
            raise FailedLambdaError()

        count = check_queue(cubes_arn)
        log.debug("Status polling - count {}".format(count))

        log.debug("Throttling count {}".format(lambda_throttle_count(lambda_arn)))

        if count == previous_count:
            count_count += 1
            if count_count == UNCHANGING_MAX:
                raise ResolutionHierarchyError("Status polling stuck at {} items for {}".format(count, polling_start - datetime.now()))
            if count_count == UNCHANGING_THROTTLE:
                # If the throttle count is increasing -> Sleep
                # If the throttle count is decreasing
                #     If the cubes queue count has changed -> Continue regular polling
                #     If the cubes queue count has not changed -> Sleep
                # If the throttle count is zero -> Continue regular polling
                #
                # This means that this loop will block until throttle has stopped / cubes
                # in the queue have been processed.
                #
                # If throttling stops and no cubes have been processed the UNCHANGING_MAX
                # threashold is the last guard so the activity doesn't hang
                prev_throttle = 0
                while True:
                    throttle = lambda_throttle_count(lambda_arn)

                    if throttle < prev_throttle and check_queue(cubes_arn) != count:
                        # If the throttle count is decreasing and the queue count has
                        # changed continue the regular polling cycle
                        break
                    if throttle == 0:
                        # No throttling happening
                        break

                    if throttle > 0:
                        # Don't update count is there was an error getting the current count
                        prev_throttle = throttle

                    # Tell SQS we're still alive
                    update_visibility_timeout(downsample_queue_url, receipt_handle)
                    time.sleep(MAX_LAMBDA_TIME.seconds)

                    if check_queue(dlq_arn) > 0:
                        raise FailedLambdaError()

            if count_count == UNCHANGING_LAUNCH:
                # We have noticed that the last few messages are spread across multiple AWS queue servers and
                # A single lambda requesting 10 messages will only get messages from a single queue server.  So we
                # pad the number of lambdas by EXTRAS_LAMBDAS to avoid extra looping cycles.
                needed = ceildiv(count, BUCKET_SIZE)
                if needed > 0:
                    log.debug("Launching {} more lambdas".format(needed))

                    start = datetime.now()
                    invoke_lambdas(needed + EXTRA_LAMBDAS, lambda_arn, lambda_args, dlq_arn)
                    stop = datetime.now()
                    log.debug("Launched {} lambdas with {} extra in {}".format(needed, EXTRA_LAMBDAS, stop - start))
        else:
            previous_count = count
            count_count = 1

        if count == 0:
            zero_count += 1
            if zero_count == ZERO_COUNT:
                log.info("Finished polling for lambda completion")
                break
            else:
                log.info("Zero cubes left, waiting to make sure lambda finishes")
        else:
            zero_count = 0

        # Tell SQS we're still alive
        update_visibility_timeout(downsample_queue_url, receipt_handle)
        time.sleep(MAX_LAMBDA_TIME.seconds)

def invoke_lambdas(count, lambda_arn, lambda_args, dlq_arn):
    """Multiprocessing.Pool worker function for invoking a number of lambdas

    Called by launch_lambdas()

    The dlq_arn queue is only checked every 10 lambdas launched. This is so
    that the queue is not hit too hard when invoking a large number of lambdas
    via Multiprocessing.Pool.

    Args:
        count (int): The number of lambdas to launch
        lambda_arn (str): Name or ARN of the lambda function to invoke
        lambda_args (str): The lambda payload to pass when invoking
        dlq_arn (str): ARN of the SQS DLQ to monitor for error messages
    """
    try:
        lambda_ = aws.get_session().client('lambda')

        log.info("Launching {} lambdas".format(count))

        for i in range(1, count+1):
            if i % 500 == 0:
                log.debug("Launched {} lambdas".format(i))
            if i % 10 == 0:
                if check_queue(dlq_arn) > 0:
                    raise FailedLambdaError()

            lambda_.invoke(FunctionName = lambda_arn,
                           InvocationType = 'Event', # Async execution
                           Payload = lambda_args)
    except Exception as ex:
        log.exception("Error caught in process, raising to controller")
        raise ResolutionHierarchyError(str(ex))

def create_queue(queue_name):
    """Create a SQS queue

    Args:
        queue_name (str): Name of the queue

    Return:
        str: URL of the queue
    """
    session = aws.get_session()
    sqs = session.client('sqs')

    resp = sqs.create_queue(QueueName = queue_name)

    url = resp['QueueUrl']
    return url

def delete_queue(queue_arn):
    """Delete a SQS queue

    Args:
        queue_arn (str): The URL of the queue
    """
    session = aws.get_session()
    sqs = session.client('sqs')

    try:
        resp = sqs.delete_queue(QueueUrl = queue_arn)
    except:
        log.exception("Could not delete status queue '{}'".format(queue_arn))

def check_queue(queue_arn):
    """Get the count of messages in the given queue

    The count is a combination Approximate Number of Messages, Messages Delayed,
    and Messages Not Visible.

    If the queue_arn contains 'dlq' the first message will be read and the SNS
    DLQ message decoded to print the error that caused the DLQ message.

    Args:
        queue_arn (str): The URL of the queue

    Return:
        int: The count of messages or zero if the queue could not be queried
    """
    session = aws.get_session()
    sqs = session.client('sqs')

    try:
        resp = sqs.get_queue_attributes(QueueUrl = queue_arn,
                                        AttributeNames = ['ApproximateNumberOfMessages',
                                                          'ApproximateNumberOfMessagesDelayed',
                                                          'ApproximateNumberOfMessagesNotVisible'])
    except:
        log.exception("Could not get message count for queue '{}'".format(queue_arn))
        return 0
    else:
        # Include both the number of messages and the number of in-flight messages
        message_count = int(resp['Attributes']['ApproximateNumberOfMessages']) + \
                        int(resp['Attributes']['ApproximateNumberOfMessagesDelayed']) + \
                        int(resp['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        if message_count > 0 and 'dlq' in queue_arn:
            try:
                resp = sqs.receive_message(QueueUrl = queue_arn)
                for msg in resp['Messages']:
                    body = json.loads(msg['Body'])
                    error = body['Records'][0]['Sns']['MessageAttributes']['ErrorMessage']['Value']
                    log.debug("DLQ Error: {}".format(error))
            except Exception as err:
                log.exception("Problem getting DLQ error message: {}".format(err))
        return message_count

def lambda_throttle_count(lambda_arn):
    """Read the Throttle count for the given Lambda function from Cloud Watch

    The metric is read for the last minute, which is in the process of being updated
    so the value read is not the final value that is recorded in Cloud Watch for the
    given time period.

    Args:
        lambda_arn (str): ARN or Name of the lambda to get the metric for

    Return:
        float: The Sample Count for the Lambda's Throttle count
        -1: If there was an error getting the metric
    """
    session = aws.get_session()
    cw = session.client('cloudwatch')

    lambda_name = lambda_arn.split(':')[-1]

    try:
        end = datetime.now()
        begin = end - timedelta(minutes=1)
        resp = cw.get_metric_statistics(Namespace = 'AWS/Lambda',
                                        MetricName = 'Throttles',
                                        # Limit the throttle count to only our target lambda function
                                        Dimensions = [{'Name': 'FunctionName', 'Value': lambda_name}],
                                        StartTime = begin,
                                        EndTime = end,
                                        Period = 60,
                                        Unit = 'Count',
                                        Statistics = ['SampleCount'])

        if 'Datapoints' in resp and len(resp['Datapoints']) > 0:
            if 'SampleCount' in resp['Datapoints'][0]:
                return resp['Datapoints'][0]['SampleCount']
        return 0.0
    except Exception as err:
        log.exception("Problem getting Lambda Throttle Count: {}".format(err))
        return -1
