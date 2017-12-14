# Lambda to write the morton index of a cuboid object key to the id in the
# DynamoDB id index table.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'cuboid_object_key': '...',
#   'id': '...',
#   'version': '...'
# }

from bossutils.aws import get_region
from spdb.spatialdb.object_indices import ObjectIndices

def handler(event, context):
    id_index_table = event['config']['object_store_config']['id_index_table']
    s3_index_table = event['config']['object_store_config']['s3_index_table']
    id_count_table = event['config']['object_store_config']['id_count_table']
    cuboid_bucket = event['config']['object_store_config']['cuboid_bucket']

    id_index_new_chunk_threshold = (
        event['config']['object_store_config']['id_index_new_chunk_threshold'])

    obj_ind = ObjectIndices(
        s3_index_table, id_index_table, id_count_table, cuboid_bucket, 
        get_region())

    obj_ind.write_id_index(
        id_index_new_chunk_threshold, 
        event['cuboid_object_key'], event['id'], event['version'])

