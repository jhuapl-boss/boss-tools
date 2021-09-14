# Lambda that starts a fanout of step functions that batch enqueue cuboid
# object keys so that they can be indexed.
#
# Used by Index.FanoutEnqueueCuboids (index_fanout_enqueue_cuboids.hsd).
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'obj_keys': [{'object-key': {'S': '...'}, 'version-node': {'N', '...'}}, ...],
#   'batch_enqueue_cuboids_step_fcn': '...',
#   'finished': '...', # boolean
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   },
#   "operation": ...,                       # (str) Op name for deadletter queue.
#   "index_ids_sqs_url": string,
#   "num_ids_per_msg": int,
#   "id_chunk_size": int,
#   "wait_time": int,
#   "id_cuboid_supervisor_step_fcn": "arn:aws:states:...",
#   "id_index_step_fcn": "arn:aws:states:...",
#   "index_ids_sqs_url": "https://queue.amazonaws.com/...",
# }
#
# Outputs:
# {
#   "operation": ...                        # (str) Op name for deadletter queue.
#   'finished': bool    # Whether done starting Index.EnqueueCuboids step functions.
#   'obj_keys': [],
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   }
######## Below are inputs to the pass to fanout_nonblocking().
#   "id_cuboid_supervisor_step_fcn": "arn:aws:states:...",
#   "id_index_step_fcn": "arn:aws:states:...",
#   "index_ids_sqs_url": "https://queue.amazonaws.com/...",
#   "num_ids_per_msg": int,
#   "id_chunk_size": int,
#   "wait_time": int,
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
    if 'sub_args' not in event and len(event['obj_keys']) == 0:
        # Must have gotten bad lookup_key because no obj_keys given on entry
        # to step function.  On subsequent calls to this lambda, obj_keys will
        # be empty but sub_args will be present.
        return {'finished': True}

    fanout_args = event

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_common = {
        'operation': 'batch_enqueue_cuboids',
        'config': event['config'],
        # Flag to tell step function's choice state that all messages enqueued.
        'enqueue_done': False,
        'index_ids_sqs_url': event['index_ids_sqs_url'],
        'num_ids_per_msg': event['num_ids_per_msg'],
        'id_chunk_size': event['id_chunk_size'],
        'wait_time': event['wait_time'],
    }

    # Add remaining arguments for fanning out.
    fanout_args['operation'] = event['operation']
    fanout_args['sub_sfn'] = event['batch_enqueue_cuboids_step_fcn']
    fanout_args['sub_sfn_is_full_arn'] = True,
    fanout_args['common_sub_args'] = fanout_subargs_common
    fanout_args['sub_args'] = build_subargs(event)
    fanout_args['max_concurrent'] = event['fanout_params']['max_concurrent']
    fanout_args['rampup_delay'] = event['fanout_params']['rampup_delay']
    fanout_args['rampup_backoff'] = event['fanout_params']['rampup_backoff']
    fanout_args['status_delay'] = event['fanout_params']['status_delay']
    fanout_args['running'] = []
    fanout_args['results'] = []
    fanout_args['finished'] = False

    if 'running' in event:
        fanout_args['running'] = event['running']

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
    if len(event['obj_keys']) > 0:
        return build_subargs_from_obj_keys(event)

    # Must be a subsequent invocation after starting fanout.
    return event['sub_args']


def build_subargs_from_obj_keys(event):
    """
    Build the array of arguments for each step function that will be spawned
    during the fanout.  This is called on the first invocation of the lambda
    function.  Before returning, during the first invocation, 'obj_keys' is 
    set to the empty list.

    Args:
        event (dict): Incoming data from the lambda function.

    Returns:
        ([dict]): Array of dictionaries, one per step function to invoke.
    """
    fanout_subargs = []
    count = 0
    args_n = {'cuboid_msgs': []}
    for obj_key in event['obj_keys']:
        data = {
            'config': event['config'],
            'cuboid': obj_key,
            'sfn_arn': event['id_cuboid_supervisor_step_fcn'],
            'id_index_step_fcn': event['id_index_step_fcn'],
            'num_ids_per_msg': event['num_ids_per_msg'],
            'id_chunk_size': event['id_chunk_size'],
            'wait_time': event['wait_time'],

        }
        args_n['cuboid_msgs'].append({
            'Id': str(count),
            'MessageBody': json.dumps(data)
        })
        if count > 0 and (count+1) % SQS_MAX_BATCH == 0:
            fanout_subargs.append(args_n)
            args_n = {'cuboid_msgs': []}
        count += 1

    # Save the last set of keys.
    remaining_keys = len(args_n['cuboid_msgs'])
    if remaining_keys > 0 and remaining_keys % SQS_MAX_BATCH != 0:
        fanout_subargs.append(args_n)

    return fanout_subargs

