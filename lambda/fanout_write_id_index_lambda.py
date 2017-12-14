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
#   'finished': ... # boolean
# }

import copy
from heaviside.activities import fanout_nonblocking

def handler(event, context):

    # 'operation' is used to identify what failed when writing to the
    # index deadletter queue.
    fanout_subargs_tmpl = {
        'operation': 'write_id_index',
        'config': event['config'],
        'cuboid_object_key': event['cuboid_object_key'],
        'version': event['version']}

    fanout_subargs = []
    for obj_id in event['ids']:
        args_n = copy.deepcopy(fanout_subargs_tmpl)
        args_n['id'] = obj_id
        fanout_subargs.append(args_n)

    fanout_args = {
        'cuboid_object_key': event['cuboid_object_key'],
        'version': event['version'],
        'config': event['config'],
        # Only send ids once so we don't fanout indefinitely.
        'ids': [],
        'id_index_step_fcn': event['id_index_step_fcn'],
        'sub_sfn': event['id_index_step_fcn'],
        'sub_args': fanout_subargs,
        'max_concurrent': 50,
        'rampup_delay': 2,
        'rampup_backoff': 0.1,
        'status_delay': 1,
        'running': [],
        'results': [],
        'finished': False
        }

    return fanout_nonblocking(fanout_args)
