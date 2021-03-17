# Lambda to write all ids contained in a cuboid to the cuboid's object key in
# the DynamoDB S3 index table.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...',
#   'cuboid_object_key': '...',
#   'version': '...',
#   'fanout_id_writers_step_fcn': '...',
#   'max_write_id_index_lambdas': int
#
# Output (additions/modifications):
# {
#   'num_ids': int,                     # Number of unique ids in cuboid.
#   'finished': False if num_ids > 0
# }
#
# Step function should abort on these errors:
#   NoSuchKey
# }

from bossutils.aws import get_region
from spdb.spatialdb.object_indices import ObjectIndices

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

    obj_ind = ObjectIndices(
        s3_index_table, id_index_table, id_count_table, cuboid_bucket, get_region())
    ids_list = obj_ind.write_s3_index(
        event['cuboid_object_key'], event['version'])

    num_ids = len(ids_list)
    event['num_ids'] = num_ids
    event['finished'] = num_ids == 0

    return event
