# Lambda that starts the Index.Supervisor step function.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_supervisor_step_fcn': ...,
#   "id_index_step_fcn": "arn:aws:states:...",
#   "batch_enqueue_cuboids_step_fcn": "arn:aws:states:...",
#   "fanout_enqueue_cuboids_step_fcn": "arn:aws:states:...",
#   "index_enqueue_cuboids_step_fcn": "arn:...",
#   "index_dequeue_cuboids_step_fcn": "arn:...",
#   "id_cuboid_supervisor_step_fcn": "arn:...",
#   "fanout_id_writers_step_fcn": "arn:...",
#   "cuboid_ids_bucket": "...",
#   "max_cuboid_fanout": int
#   "max_write_id_index_lambdas": int
# }
#
# Outputs (all inputs with the following additions):
# {
#   "sfn_arn": ...,      # arn of Index.Supervisor step function.
#   "queue_empty": false,
#   "sfn_output": {
#     "executionArn": "...",
#     "startDate": "..."
#   }
# }

import start_sfn_lambda

def handler(event, context):

    # Expected by initial choice state of Index.Supervisor.
    event['queue_empty'] = False

    # Tell start_sfn_lambda which step function to start.
    event['sfn_arn'] = event['id_supervisor_step_fcn']

    return start_sfn_lambda.handler(event, context)

