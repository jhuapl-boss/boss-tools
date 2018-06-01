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

from bossutils import aws, logger
from spdb.c_lib.ndtype import CUBOIDSIZE

from bossutils.multidimensional import XYZ, ceildiv
from bossutils.multidimensional import range as xyz_range

import types
import json
import time
import random
from multiprocessing import Pool, Value, cpu_count
from datetime import timedelta, datetime

log = logger.BossLogger().logger

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
###########################################################

class ResolutionHierarchyError(Exception):
    pass

class LambdaLaunchError(ResolutionHierarchyError):
    pass

class FailedLambdaError(ResolutionHierarchyError):
    def __init__(self):
        msg = "A Lambda failed execution"
        super().__init__(msg)

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
        }
    """

    # TODO: load downsample_volume_lambda from boss config

    # Different ID and queue for each resolution, as it takes 60 seconds to delete a queue
    downsample_id = str(random.random())[2:] # remove the '0.' part of the number
    dlq_arn = create_queue('downsample-dlq-' + downsample_id)
    cubes_arn = create_queue('downsample-cubes-' + downsample_id)

    try:
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

            if resolution >= args['iso_resolution']: # DP TODO: Figure out how to launch aniso iso version with mutating arguments
                configs.append({
                    'name': 'isotropic',
                    'step': XYZ(2,2,2),
                    'iso_flag': True,
                    'frame_start_key': 'iso_{}_start',
                    'frame_stop_key': 'iso_{}_stop',
                })

        for config in configs:
            frame_start = frame(config['frame_start_key'])
            frame_stop = frame(config['frame_stop_key'])
            step = config['step']
            use_iso_flag = config['iso_flag'] # If the resulting cube should be marked with the ISO flag

            # Round to the furthest full cube from the center of the data
            cubes_start = frame_start // dim
            cubes_stop = ceildiv(frame_stop, dim)

            log.debug('Downsampling {} resolution {}'.format(config['name'], resolution))
            log.debug("Frame corner: {}".format(frame_start))
            log.debug("Frame extent: {}".format(frame_stop))
            log.debug("Cubes corner: {}".format(cubes_start))
            log.debug("Cubes extent: {}".format(cubes_stop))
            log.debug("Downsample step: {}".format(step))

            log.debug("Populating input cube")
            cube_count = populate_cubes(cubes_arn, cubes_start, cubes_stop, step)

            log.debug("Invoking downsample lambdas")
            lambda_count = ceildiv(cube_count, BUCKET_SIZE)
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
                           cubes_arn)

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

        # if next iteration will split into aniso and iso downsampling, copy the coordinate frame
        if args['type'] != 'isotropic' and (resolution + 1) == args['iso_resolution']:
            def copy(var):
                args['iso_{}_start'.format(var)] = args['{}_start'.format(var)]
                args['iso_{}_stop'.format(var)] = args['{}_stop'.format(var)]
            copy('x')
            copy('y')
            copy('z')

        # Advance the loop and recalculate the conditional
        # Using max - 1 because resolution_max should not be a valid resolution
        # and res < res_max will end with res = res_max - 1, which generates res_max resolution
        args['resolution'] = resolution + 1
        args['res_lt_max'] = args['resolution'] < (args['resolution_max'] - 1)
        return args
    finally:
        delete_queue(dlq_arn)
        delete_queue(cubes_arn)

def chunk(xs, size):
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
    """
    extents = (stop - start) / step
    return int(extents.x * extents.y * extents.z)

def make_cubes(start, stop, step):
    for target in xyz_range(start, stop, step = step):
        yield target # XYZ type is automatically handled by JSON.dumps
                     # Since it is a subclass of tuple

def populate_cubes(queue_arn, start, stop, step):
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

def launch_lambdas(total_count, lambda_arn, lambda_args, dlq_arn, cubes_arn):
    per_lambda = ceildiv(total_count, POOL_SIZE)
    d,m = divmod(total_count, per_lambda)
    counts = [per_lambda] * d + [m]

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

        log.debug("Throttling count {}".format(lambda_throttle_count()))

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
                    throttle = lambda_throttle_count()

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

                    time.sleep(MAX_LAMBDA_TIME.seconds)

                    if check_queue(dlq_arn) > 0:
                        raise FailedLambdaError()

            if count_count == UNCHANGING_LAUNCH:
                needed = ceildiv(count, BUCKET_SIZE)
                if needed > 0:
                    log.debug("Launching {} more lambdas".format(needed))

                    start = datetime.now()
                    invoke_lambdas(needed, lambda_arn, lambda_args, dlq_arn)
                    stop = datetime.now()
                    log.debug("Launched {} lambdas in {}".format(needed, stop - start))
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

        time.sleep(MAX_LAMBDA_TIME.seconds)

def invoke_lambdas(count, lambda_arn, lambda_args, dlq_arn):
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

def create_queue(queue_name, fifo=False):
    session = aws.get_session()
    sqs = session.client('sqs')
    attributes = {}

    if fifo:
        queue_name += '.fifo'
        attributes = {
            'FifoQueue': 'true',
            'ContentBasedDeduplication': 'true',
        }

    resp = sqs.create_queue(QueueName = queue_name,
                            Attributes = attributes)
    url = resp['QueueUrl']
    return url

def delete_queue(queue_arn):
    session = aws.get_session()
    sqs = session.client('sqs')

    try:
        resp = sqs.delete_queue(QueueUrl = queue_arn)
    except:
        log.exception("Could not delete status queue '{}'".format(queue_arn))

def check_queue(queue_arn):
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
            except:
                log.exception("Problem gettting DLQ error message")
        return message_count

def lambda_throttle_count():
    session = aws.get_session()
    cw = session.client('cloudwatch')

    try:
        now = datetime.now()
        delta = timedelta(minutes=1)
        resp = cw.get_metric_statistics(Namespace = 'AWS/Lambda',
                                        MetricName = 'Throttles',
                                        StartTime = now - delta,
                                        EndTime = now,
                                        Period = 60,
                                        Statistics = ['Average'])

        if 'Datapoints' in resp and len(resp['Datapoints']) > 0:
            if 'Average' in resp['Datapoints'][0]:
                return resp['Datapoints'][0]['Average']
        return 0
    except:
        log.exception("Problem getting Lambda Throttle Count")
        return -1
