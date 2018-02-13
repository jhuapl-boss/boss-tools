# Lambda to write the morton index of a cuboid object key to the id in the
# DynamoDB id index table.
#
# It expects to get from events dictionary
# {
#   'id_index_table': ...,
#   's3_index_table': ...,
#   'id_count_table': ...,
#   'cuboid_bucket': ...,
#   'id_index_new_chunk_threshold': ...,
#   'cuboid_object_key': '...',
#   'id_group': '...',
#   'version': '...'
# }

from bossutils.aws import get_region
from spdb.spatialdb.object_indices import ObjectIndices

def handler(event, context):
    id_index_table = event['id_index_table']
    s3_index_table = event['s3_index_table']
    id_count_table = event['id_count_table']
    cuboid_bucket = event['cuboid_bucket']

    id_index_new_chunk_threshold = (event['id_index_new_chunk_threshold'])

    obj_ind = ObjectIndices(
        s3_index_table, id_index_table, id_count_table, cuboid_bucket, 
        get_region())

    for obj_id in event['id_group']:
        obj_ind.write_id_index(
            id_index_new_chunk_threshold, 
            event['cuboid_object_key'], obj_id, event['version'])

