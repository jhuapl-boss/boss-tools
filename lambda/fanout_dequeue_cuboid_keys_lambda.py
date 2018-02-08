# Lambda that fansout step functions to start pulling batches of messages
# from the cuboids keys (Cuboidkeys) queue.  These step functions also
# start the Index.CuboidSupervisor step function which starts the actual
# indexing operation.
#
# Required inputs:
# {
#   "config": {
#     "object_store_config": {
#       "index_cuboids_keys_queue": "..."
#     }
#   },
#   "ApproximateNumberOfMessages": ...      # (int) # msgs in queue.
#   "max_cuboid_fanout": ...                # (int) Max # of cuboids to start indexing.
#   "index_dequeue_cuboids_step_fcn": ...   # (str) Arn of Index.DequeueCuboids step function.
#   "id_cuboid_supervisor_step_fcn": ...    # (str) Arn of Index.CuboidSupervisor step function.
#   "id_index_step_fcn": ...                # (str) Arn of Index.IdWriter step function.
# }
#
#
# Outputs:
# {
#   'finished': bool    # Whether done starting Index.DequeueCuboids step functions.
#   'ApproximateNumberOfMessages': 0,
######## Below are inputs to the pass to fanout_nonblocking().
#   'cuboid_object_key': '...',
#   'version': '...',
#   'config': ...
#   'sub_sfn': ...
#   'sub_args': [...]
#   'max_concurrent': ...,
#   'rampup_delay': ...,
#   'rampup_backoff': ...,
#   'status_delay': ...
#   'running': [...],
#   'results': [...]
# }

import copy
from heaviside.activities import fanout_nonblocking
from math import ceil

SQS_MAX_RCV = 10

def handler(event, context):
    queue = event['config']['object_store_config']['index_cuboids_keys_queue']
    num_msgs = event['ApproximateNumberOfMessages']

    max_to_spawn = event['max_cuboid_fanout']
    sfns_to_spawn = min(ceil(num_msgs/SQS_MAX_RCV), max_to_spawn )
    
    fanout_args = event

    # Add remaining arguments for fanning out.
    fanout_args['sub_sfn'] = event['index_dequeue_cuboids_step_fcn']
    fanout_args['sub_args'] = build_subargs(event, sfns_to_spawn)
    fanout_args['max_concurrent'] = 50
    fanout_args['rampup_delay'] = 2
    fanout_args['rampup_backoff'] = 0.1
    fanout_args['status_delay'] = 1
    fanout_args['running'] = []
    fanout_args['results'] = []
    fanout_args['finished'] = False

    # Zero this out so we don't infinitely fanout.
    fanout_args['ApproximateNumberOfMessages'] = 0

    return fanout_nonblocking(fanout_args)


def build_subargs(event, num_to_fanout):
    """
    Build the array of arguments for each step function that will be spawned
    during the fanout.

    Args:
        event (dict): Incoming data from the lambda function.
        num_to_fanout (int): Number of step functions to create.

    Returns:
        ([dict]): Array of dictionaries, one per step function to invoke.
    """

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_tmpl = {
        'operation': 'start_indexing_cuboid',
        'config': event['config'],
        'id_cuboid_supervisor_step_fcn': event['id_cuboid_supervisor_step_fcn'],
        'id_index_step_fcn': event['id_index_step_fcn']
    }

    fanout_subargs = []
    for i in range(0, num_to_fanout):
        args_n = copy.deepcopy(fanout_subargs_tmpl)
        fanout_subargs.append(args_n)

    return fanout_subargs
