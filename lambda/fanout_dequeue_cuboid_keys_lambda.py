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
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   },
#   "operation": ...                        # (str) Op name for deadletter queue.
# }
#
#
# Outputs:
# {
#   "operation": ...                        # (str) Op name for deadletter queue.
#   'finished': bool    # Whether done starting Index.DequeueCuboids step functions.
#   'ApproximateNumberOfMessages': 0,
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   }
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

    # Each Index.DequeueCuboids step function will start SQS_MAX_RCV 
    # Index.CubiodSupervisors, so divide 'max_cuboid_fanout' by SQS_MAX_RCV.
    max_to_spawn = ceil(event['max_cuboid_fanout']/SQS_MAX_RCV)

    sfns_to_spawn = min(ceil(num_msgs/SQS_MAX_RCV), max_to_spawn)
    
    fanout_args = event

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_common = {
        'operation': 'start_indexing_cuboid',
        'config': event['config'],
        'id_cuboid_supervisor_step_fcn': event['id_cuboid_supervisor_step_fcn'],
        'id_index_step_fcn': event['id_index_step_fcn'],
        'fanout_id_writers_step_fcn': event['fanout_id_writers_step_fcn'],
        'max_write_id_index_lambdas': event['max_write_id_index_lambdas']
    }

    # Add remaining arguments for fanning out.
    fanout_args['operation'] = event['operation']
    fanout_args['sub_sfn'] = event['index_dequeue_cuboids_step_fcn']
    fanout_args['sub_sfn_is_full_arn'] = True,
    fanout_args['common_sub_args'] = fanout_subargs_common
    # No unique arguments required, so just send a list of empty dicts.
    fanout_args['sub_args'] = [{} for i in range(0, sfns_to_spawn)]
    fanout_args['max_concurrent'] = event['fanout_params']['max_concurrent']
    fanout_args['rampup_delay'] = event['fanout_params']['rampup_delay']
    fanout_args['rampup_backoff'] = event['fanout_params']['rampup_backoff']
    fanout_args['status_delay'] = event['fanout_params']['status_delay']
    fanout_args['running'] = []
    fanout_args['results'] = []
    fanout_args['finished'] = False

    if 'running' in event:
        fanout_args['running'] = event['running']

    # Zero this out so we don't infinitely fanout.
    fanout_args['ApproximateNumberOfMessages'] = 0

    return fanout_nonblocking(fanout_args)

