# Lambda that fanouts Index.FanoutIdWriters based on the number of unique ids
# found in the cuboid.  Ids divided among multiple workers because step
# function data passed between states must be less than 32K.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...', # arn of step function
#   'fanout_id_writers_step_fcn': '...',    # step func arn
#   'cuboid_object_key': '...',
#   'num_ids': int,             # Number of unique ids in cuboid.
#   'version': '...',
#   'max_write_id_index_lambdas': int,
#   'finished': '...', # bool
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   },
#   "index_ids_sqs_url": string,
#   "num_ids_per_msg": int,
#   "id_chunk_size": int,
#   "wait_time": int,
# }
#
# Outputs (modified or added):
# {
#   'finished': bool    # Whether done starting Index.FanoutIdWriter step functions.
#   'num_ids': 0,       # Zeroed to avoid infinite fanning out.
#   'wait_secs': int    # Tell subsequent wait state how long to wait.
#   'sub_sfn': ...
#   'sub_args': [...]
#   'max_concurrent': ...,
#   'rampup_delay': ...,
#   'rampup_backoff': ...,
#   'status_delay': ...
#   'running': [...]
#   'results': [...]
# }

from cloudwatchwrapper import get_lambda_execs_in_last_min
from heaviside.activities import fanout_nonblocking
from math import ceil
from random import randint

NUM_IDS_PER_WORKER = 100

def handler(event, context):

    num_ids = int(event['num_ids'])
    if num_ids == 0:
        if 'sub_args' not in event or len(event['sub_args']) == 0:
            # Nothing left to do.
            output = {'finished': True}
            return output

    fanout_args = get_fanout_args(event)

    # Change this key?
    max_execs = int(event['max_write_id_index_lambdas']) 

    num_execs = get_lambda_execs_in_last_min()
    if max_execs - num_execs < len(fanout_args['sub_args']) * 2:
        # Don't fanout during this execution.  Wait for more lambda 
        # availability.
        fanout_args['wait_secs'] = 10 + randint(15, 25)
        print('Too many lambdas, pausing fanout for {} secs.'.format(
            fanout_args['wait_secs']))
        return fanout_args

    return fanout_nonblocking(fanout_args)


def get_fanout_args(event):
    """
    Generates arguments for fanout.  Starts with the event dictionary as its
    base and augments with additional arguments.

    Args:
        (dict): The dictionary passed in the lambda handler.

    Returns:
        (dict): Dictionary with arguments necessary for fanout.
    """
    num_ids = int(event['num_ids'])
    if num_ids == 0:
        fanout_subargs = event['sub_args']
    else:
        num_workers = ceil(num_ids/NUM_IDS_PER_WORKER)
        # Each worker uses worker_id to index into the id list.
        fanout_subargs = [{'worker_id': i} for i in range(0, num_workers)]

    max_execs = int(event['max_write_id_index_lambdas']) 

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_common = {
        'operation': 'split_fanout_cuboid_supervisors',
        'config': event['config'],
        'cuboid_object_key': event['cuboid_object_key'],
        'version': event['version'],
        'max_write_id_index_lambdas': max_execs,
        'id_index_step_fcn': event['id_index_step_fcn'],
        'num_ids_per_worker': NUM_IDS_PER_WORKER,
        'index_deadletter_queue': event['config']['object_store_config']['index_deadletter_queue'],
        'index_ids_sqs_url': event['index_ids_sqs_url'],
        'num_ids_per_msg': event['num_ids_per_msg'],
        'id_chunk_size': event['id_chunk_size'],
        'wait_time': event['wait_time'],
    }

    fanout_args = event
    fanout_args['operation'] = event['operation']
    # Zero this out so we don't fanout indefinitely.
    fanout_args['num_ids'] = 0
    fanout_args['sub_sfn'] = event['fanout_id_writers_step_fcn']
    fanout_args['sub_sfn_is_full_arn'] = True
    fanout_args['common_sub_args'] = fanout_subargs_common
    fanout_args['sub_args'] = fanout_subargs
    fanout_args['max_concurrent'] = event['fanout_params']['max_concurrent']
    fanout_args['rampup_delay'] = event['fanout_params']['rampup_delay']
    fanout_args['rampup_backoff'] = event['fanout_params']['rampup_backoff']
    fanout_args['status_delay'] = event['fanout_params']['status_delay']
    # Purposely emptying running list.  Limits on checking on running
    # executions too small for the amount of Index.CuboidSupervisors that 
    # will be simultaneously calling fanout_nonblocking().
    fanout_args['running'] = []
    fanout_args['results'] = []
    fanout_args['finished'] = False
    fanout_args['wait_secs'] = 10

    return fanout_args
