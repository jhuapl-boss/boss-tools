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

from heaviside.activities import fanout

log = logger.BossLogger().logger

# Fanout variables, see heaviside.activities.fanout for full details
# int - seconds: The delay between launch subprocesses and polling for status
POLL_DELAY = 5

# int - seconds: The inner delay between status queries
#                Helps limit API speed, so as to not run into throttling issues
STATUS_DELAY = 1

# int: Maximum number of concurrent subprocess executions to have running
MAX_NUM_PROCESSES = 50

# int - seconds: The initial rampup delay to allow AWS resources to scale
RAMPUP_DELAY = 15

# float: The backoff value to multiple RAMPUP_DELAY by for each subprocess launch
#        When RAMPUP_DELAY is zero there is no longer a delay between launched
RAMPUP_BACKOFF = 0.8

def downsample_channel(args):
    """
    Slice the given channel into chunks of 2x2x2 or 2x2x1 cubes that are then
    sent to the downsample_volume lambda for downsampling into a 1x1x1 cube at
    resolution + 1.

    Makes use of the bossutils.multidimensional library for simplified vector
    math.

    Args:
        args {
            downsample_volume_sfn (ARN)

            collection_id (int)
            experiment_id (int)
            channel_id (int)
            annotation_channel (bool)
            data_type (str) 'uint8' | 'uint16' | 'uint64'

            s3_bucket (URL)
            s3_index (URL)
            id_index (URL)

            x_start (int)
            y_start (int)
            z_start (int)

            x_stop (int)
            y_stop (int)
            z_stop (int)

            resolution (int) The resolution to downsample. Creates resolution + 1
            resolution_max (int) The maximum resolution to generate
            res_lt_max (bool) = args['resolution'] < (args['resolution_max'] - 1)

            annotation_index_max (int) The maximum resolution to index annotation channel cubes at
                                       When annotation_index_max = N, indices will exist for res 0 - (N - 1)

            type (str) 'isotropic' | 'anisotropic'
            iso_resolution (int) if resolution >= iso_resolution && type == 'anisotropic' downsample both
        }
    """

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
        index_annotations = args['resolution'] < (args['annotation_index_max'] - 1)

        # Round to the furthest full cube from the center of the data
        cubes_start = frame_start // dim
        cubes_stop = ceildiv(frame_stop, dim)

        log.debug('Downsampling {} resolution {}'.format(config['name'], resolution))
        log.debug("Frame corner: {}".format(frame_start))
        log.debug("Frame extent: {}".format(frame_stop))
        log.debug("Cubes corner: {}".format(cubes_start))
        log.debug("Cubes extent: {}".format(cubes_stop))
        log.debug("Downsample step: {}".format(step))
        log.debug("Indexing Annotations: {}".format(index_annotations))

        # Call the downsample_volume lambda to process the data
        fanout(aws.get_session(),
               args['downsample_volume_sfn'],
               make_args(args, cubes_start, cubes_stop, step, dim, use_iso_flag, index_annotations),
               max_concurrent = MAX_NUM_PROCESSES,
               rampup_delay = RAMPUP_DELAY,
               rampup_backoff = RAMPUP_BACKOFF,
               poll_delay = POLL_DELAY,
               status_delay = STATUS_DELAY)

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

def make_args(args, start, stop, step, dim, use_iso_flag, index_annotations):
    for target in xyz_range(start, stop, step = step):
        yield {
            'lambda-name' : 'downsample_volume', # name of the function in multiLambda to call
            'args': args,
            'target': target, # XYZ type is automatically handled by JSON.dumps
            'step': step,     # Since it is a subclass of tuple
            'dim': dim,
            'use_iso_flag': use_iso_flag,
            'index_annotations': index_annotations,
        }

