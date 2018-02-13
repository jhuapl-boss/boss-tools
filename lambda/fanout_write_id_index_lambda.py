# Lambda that starts fanout of step functions that update the id index table.
# Each step function adds the cuboid's morton id to a single object id.
# The id index maps object ids to the cuboids that contain them.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...', # arn of step function
#   'cuboid_object_key': '...',
#   'ids': [...],
#   'version': '...',
#   'finished': '...', # bool
#   'fanout_params': {
#       "max_concurrent": int,
#       "rampup_delay": int,
#       "rampup_backoff": float,
#       "status_delay": int
#   }
# }
#
# Outputs:
# {
#   'finished': bool    # Whether done starting Index.IdWriter step functions.
#   'ids': [],
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
#   'running': [...]
#   'results': [...]
# }

from heaviside.activities import fanout_nonblocking

IDS_PER_LAMBDA = 10

def handler(event, context):

    # The cuboid had no non-zero ids so nothing to do.
    if len(event['ids']) == 0:
        if 'sub_args' not in event or len(event['sub_args']) == 0:
            output = {'finished': True}
            return output

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_common = {
        'operation': 'write_id_index',
        'id_index_table': event['config']['object_store_config']['id_index_table'],
        's3_index_table': event['config']['object_store_config']['s3_index_table'],
        'id_count_table': event['config']['object_store_config']['id_count_table'],
        'cuboid_bucket': event['config']['object_store_config']['cuboid_bucket'],
        'index_deadletter_queue': event['config']['object_store_config']['index_deadletter_queue'],
        'id_index_new_chunk_threshold': event['config']['object_store_config']['id_index_new_chunk_threshold'],
        'cuboid_object_key': event['cuboid_object_key'],
        'version': event['version']}

    if len(event['ids']) > 0:
        fanout_subargs = pack_ids_for_lambdas(event['ids'])
    else:
        fanout_subargs = event['sub_args']

    fanout_args = {
        'operation': event['operation'],
        'cuboid_object_key': event['cuboid_object_key'],
        'version': event['version'],
        'config': event['config'],
        'fanout_params': event['fanout_params'],
        # Only send ids once so we don't fanout indefinitely.
        'ids': [],
        'id_index_step_fcn': event['id_index_step_fcn'],
        'sub_sfn': event['id_index_step_fcn'],
        'sub_sfn_is_full_arn': True,
        'common_sub_args': fanout_subargs_common,
        'sub_args': fanout_subargs,
        'max_concurrent': event['fanout_params']['max_concurrent'],
        'rampup_delay': event['fanout_params']['rampup_delay'],
        'rampup_backoff': event['fanout_params']['rampup_backoff'],
        'status_delay': event['fanout_params']['status_delay'],
        'running': [],
        'results': [],
        'finished': False
        }

    return fanout_nonblocking(fanout_args)


def pack_ids_for_lambdas(ids):
    """
    Pack IDS_PER_LAMBDA ids for writing to the id index table into each lambda
    (write_id_index_lambda.py).

    Args:
        ([str]): List of ids as strings.

    Returns:
        ([dict]) Array of {'id_group': [str]} where ids are strings.

    """
    fanout_subargs = []
    id_group = []
    count = 0
    for obj_id in ids:
        if count % IDS_PER_LAMBDA == 0:
            if count > 0:
                fanout_subargs.append({'id_group': id_group})
            id_group = []

        id_group.append(obj_id)

        count += 1

    if len(id_group) > 0:
        fanout_subargs.append({'id_group': id_group})

    return fanout_subargs
