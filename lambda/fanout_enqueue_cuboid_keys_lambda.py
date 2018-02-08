# Lambda that starts a fanout of step functions that batch enqueue cuboid
# object keys so that they can be indexed.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'obj_keys': [{'object-key': {'S': '...'}, 'version-node': {'N', '...'}}, ...],
#   'batch_enqueue_cuboids_step_fcn': '...',
#   'finished': ... # boolean
# }
#
# Outputs:
# {
#   'finished': bool    # Whether done starting Index.EnqueueCuboids step functions.
#   'obj_keys': []
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
import json
from heaviside.activities import fanout_nonblocking

# Maximum number of messages allowed in a batch send.
SQS_MAX_BATCH = 10

def handler(event, context):
    fanout_args = event

    # Add remaining arguments for fanning out.
    fanout_args['sub_sfn'] = event['batch_enqueue_cuboids_step_fcn']
    fanout_args['sub_args'] = build_subargs(event)
    fanout_args['max_concurrent'] = 50
    fanout_args['rampup_delay'] = 2
    fanout_args['rampup_backoff'] = 0.1
    fanout_args['status_delay'] = 1
    fanout_args['running'] = []
    fanout_args['results'] = []
    fanout_args['finished'] = False

    # Don't pass object keys on so we don't fanout indefinitely.
    fanout_args['obj_keys'] = []

    return fanout_nonblocking(fanout_args)


def build_subargs(event):
    """
    Build the array of arguments for each step function that will be spawned
    during the fanout.

    Args:
        event (dict): Incoming data from the lambda function.

    Returns:
        ([dict]): Array of dictionaries, one per step function to invoke.
    """

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_tmpl = {
        'operation': 'batch_enqueue_cuboids',
        'config': event['config'],
        'cuboid_msgs': [],
        # Flag to tell step function's choice state that all messages enqueued.
        'enqueue_done': False
    }

    fanout_subargs = []
    count = 0
    args_n = copy.deepcopy(fanout_subargs_tmpl)
    for obj_key in event['obj_keys']:
        args_n['cuboid_msgs'].append({
            'Id': str(count),
            'MessageBody': json.dumps(obj_key)
        })
        if count > 0 and (count+1) % SQS_MAX_BATCH == 0:
            fanout_subargs.append(args_n)
            args_n = copy.deepcopy(fanout_subargs_tmpl)
        count += 1

    # Save the last set of keys.
    remaining_keys = len(args_n['cuboid_msgs'])
    if remaining_keys > 0 and remaining_keys % SQS_MAX_BATCH != 0:
        fanout_subargs.append(args_n)

    return fanout_subargs

