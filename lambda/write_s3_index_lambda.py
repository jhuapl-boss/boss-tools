# Lambda to write all ids contained in a cuboid to the cuboid's object key in
# the DynamoDB S3 index table.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...',
#   'cuboid': {
#       'object-key': {'S': str},
#       'version-node': {'N': str}, 
#   },
#}
#
# Output (additions/modifications):
# {
#   'worker_ids': Array[int] - input to step function map state for spawning cuboid supervisors
#   'num_ids_per_worker': int
# }
#
# Step function should abort on these errors:
#   NoSuchKey
# }

from math import ceil
import os
from spdb.spatialdb.object_indices import ObjectIndices

NUM_IDS_PER_WORKER = 100

def handler(event, context):
    """
    Write all ids in a cuboid to the S3 cuboid index so we can quickly
    retrieve the ids containined within a cuboid.

    Args:
        event (dict): Input parameters.  See comment at top for expected contents.
        context (Context): Contains runtime info about the lambda.

    Returns:
        (dict): see Output description at top of file.
    """

    id_index_table = event['config']['object_store_config']['id_index_table']
    s3_index_table = event['config']['object_store_config']['s3_index_table']
    id_count_table = event['config']['object_store_config']['id_count_table']
    cuboid_bucket = event['config']['object_store_config']['cuboid_bucket']

    region = os.environ['AWS_REGION']
    obj_ind = ObjectIndices(
        s3_index_table, id_index_table, id_count_table, cuboid_bucket, region)
    ids_list = obj_ind.write_s3_index(
        event['cuboid']['object-key']['S'], event['cuboid']['version-node']['N'])

    num_ids = len(ids_list)
    num_workers = ceil(num_ids/NUM_IDS_PER_WORKER)
    event['worker_ids'] = [w_id for w_id in range(num_workers)]
    event['num_ids_per_worker'] = NUM_IDS_PER_WORKER

    return event
