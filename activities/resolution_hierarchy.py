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

import time
import random

log = logger.BossLogger().logger

# int: Approximent maximum number of concurrent lambda executions to have running
#      Zero (0) means no limit
MAX_NUM_PROCESSES = 0

# int: The number of volumes each sub_sfn should downsample with each execution
BUCKET_SIZE = 1

# int: The number of retries for launching a lambda before givin up and failing
#      This only applies to throttling / resource related exceptions
#      Zero (0) means no limit
RETRY_LIMIT = 0

# int - seconds: The number of seconds between polling for the count in the status table
DYNAMODB_POLL = 1

class LambdaLaunchError(Exception):
    pass

class LambdaRetryLimitExceededError(Exception):
    def __init__(self):
        msg = "Number of a throttle lambda retries exceeded limit of {}"
        super().__init__(msg.format(RETRY_LIMIT))

class DynamoDBStatusError(Exception):
    pass

def downsample_channel(args):
    """
    Slice the given channel into chunks of 2x2x2 or 2x2x1 cubes that are then
    sent to the downsample_volume lambda for downsampling into a 1x1x1 cube at
    resolution + 1.

    Makes use of the bossutils.multidimensional library for simplified vector
    math.

    Args:
        args {
            downsample_id (str) (Optional, one will be generated if not provided)
            downsample_volume_lambda (ARN | lambda name)
            downsample_status_table (ARN | table name)

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

    if 'downsample_id' not in args:
        # NOTE: Right now assume that this will not produce two ids that would be executing at the same time
        args['downsample_id'] = str(random.random())

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

        sub_args = make_args(args, cubes_start, cubes_stop, step, dim, use_iso_flag)
        sub_buckets = bucket(sub_args, BUCKET_SIZE)

        launch_lambda(args['downsample_volume_lambda'],
                      args['downsample_status_table'],
                      args['downsample_id'],
                      sub_buckets)

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

def make_args(args, start, stop, step, dim, use_iso_flag):
    for target in xyz_range(start, stop, step = step):
        yield {
            'args': args,
            'target': target, # XYZ type is automatically handled by JSON.dumps
            'step': step,     # Since it is a subclass of tuple
            'dim': dim,
            'use_iso_flag': use_iso_flag,
        }

def bucket(sub_args, bucket_size):
    """Take a generator of sub_args and break into multiple lists.

    Args:
        sub_args (generator): Generator yielding sub_args dictionaries
        bucket_size (int): Maximum number of sub_arg dictionaries in each
                           bucket. There may be less if bucket_size doesn't
                           perfectly divide into the number of sub_args.

    Returns:
        (generator): Each yield is a list of sub_arg dictionaries
    """
    running = True
    while running:
        sub_args_bucket = []
        try:
            for i in range(bucket_size):
                sub_args_bucket.append(next(sub_args))
        except StopIteration:
            running = False
        # we have to yield a dict with lambda-name at the first level to work with the multiLambda
        yield {
                'lambda-name': 'downsample_volume',  # name of the function in multiLambda to call
                'bucket_args':  sub_args_bucket  # bucket of args
              }

def launch_lambda(lambda_arn, status_table, downsample_id, buckets):
    session = aws.get_session();
    lambda_ = session.client('lambda')
    ddb = session.client('dynamodb')

    slowdown = 0
    for bucket in buckets:
        if MAX_NUM_PROCESSES > 0:
            # NOTE: Not currently accounting for DLQ messages, as if there is a message
            #       then the whole downsample has failed, due to AWS automatically retrying
            #       lambdas for us (if we figure out that the AWS auto-retry abilities
            #       don't work for us then this has to be revisited)
            while status_count(status_table, downsample_id) > MAX_NUM_PROCESSES:
                time.sleep(DYNAMODB_POLL)

        retry = 0

        keys = []
        try:
            for sub_arg in bucket:
                item = {'downsample_id': {'S': downsample_id},
                        'cube_morton': {'N': sub_args.target.morton}}
                ddb.put_item(TableName = status_table, Item = item)
                keys.append(item)
        except Exception as ex: # XXX: more specific errors?
            for key in keys:
                try:
                    ddb.delete_item(TableName = status_table, Key = key)
                except:
                    log.exception("Could not delete key '{}' from downsample status table".format(key))

            raise DynamoDBStatusError("Problem creating state entries: {}".format(ex))
        else:
            try: 
                while True:
                    try:
                        time.sleep(slowdown)
                        lambda_.invoke(FunctionName = lambda_arn,
                                       InvocationType = 'Event', # Async execution
                                       Payload = json.dumps(bucket).encode('UTF8'))
                    except lambda_.RequestTooLargeException:
                        # Cannot handle these requests
                        raise LambdaLaunchError("Lambda payload is too large, unable to launch")
                    except (lambda_.TooManyRequestsException,
                            lambda_.SubnetIPAddressLimitReachedException,
                            lambda_.ENILimitReachedException):
                        # Need to wait until some executions has finished
                        log.debug("Unavailable resources, waiting for some lambdas to finish")
                        if RETRY_LIMIT > 0 and retry > RETRY_LIMIT:
                            raise LambdaRetryLimitExceededError()
                        retry += 1

                        # busy loop until the number of cubes is less than the current value
                        starting = status_count(status_table, downsample_id)
                        while True:
                            time.sleep(DYNAMODB_POLL)
                            current = status_count(status_table, downsample_id)
                            if current < starting:
                                break
                    except (lambda_.EC2ThrottledException,):
                        # Need to slow down
                        log.debug("Throttled, slowing down")
                        if RETRY_LIMIT > 0 and retry > RETRY_LIMIT:
                            raise LambdaRetryLimitExceededError()
                        retry += 1
                        slowdown += 1 # XXX: is there a better value?
                                      # XXX: how to handle decrementing slowdown value?
                                      #      maybe remove 1 after a successful launch?
            except:
                # Remove keys that were created but no lambda could be launched for
                for key in keys:
                    try:
                        ddb.delete_item(TableName = status_table, Key = key)
                    except:
                        log.exception("Could not delete key '{}' from downsample status table".format(key))

                raise

def status_count(status_table, downsample_id):
    session = aws.get_session()
    ddb = session.client('dynamodb')

    slowdown = 0
    while True:
        try:
            time.sleep(slowdown)
            resp = ddb.query(TableName = status_table,
                             Select = 'COUNT',
                             KeyConditionExpression = "downsample_id = :id",
                             ExpressionAttributeValues = {
                                 ":id": {'S': downsample_id}
                             })
            return resp['Count']
        except ddb.ProvisionedThroughputExceededException:
            log.debug("Status check throttled, slowing down")
            slowdown += 1 # XXX: is there a better value?
        except Exception as ex:
            raise DynamoDBStatusError("Could not query status table count: {}".format(ex))
