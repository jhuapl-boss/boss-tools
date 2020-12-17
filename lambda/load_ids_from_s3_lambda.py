# Lambda that loads the list of unique ids belonging to a cuboid and outputs
# a subset of those ids into chunks of ids for enqueueing in SQS.  The chunks
# of ids will be passed to a Map state that will run another lambda that does
# that enqueuing.
#
# Ids are loaded from the Dynamo table: s3index.domain.tld.
#
# Because step function states limit the amount of data passed between them,
# the ids are divvied up in this manner.
#
# Used by Index.FanoutIdWriters (index_fanout_id_writers.hsd).
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'cuboid_ids_bucket': '...',
#   'id_index_step_fcn': '...', # arn of step function
#   'cuboid_object_key': '...',
#   'version': '...',
#   'max_write_id_index_lambdas': int,
#   'worker_id': int            # Id used to index into JSON list of ids.
#   'num_ids_per_worker': int   # Number of ids this lambda will output in chunks.
#   'id_chunk_size': int        # Number of ids to include in a chunk.
# }
#
# Outputs (modified or added):
# {
#   'id_chunks': [[...], [...], ...]    # List of list of ids to update in id index table.
# }

import boto3
import math

IDSET_ATTR = "id-set"


def handler(event, context):
    num_ids_per_worker = event["num_ids_per_worker"]
    id_chunk_size = event["id_chunk_size"]
    obj_key = event["cuboid_object_key"]
    s3_index = event["config"]["object_store_config"]["s3_index_table"]
    version = str(event["version"])

    resp = read_dynamo(s3_index, obj_key, version)

    if "Item" not in resp or IDSET_ATTR not in resp["Item"]:
        # No ids found, so nothing to do.
        event["id_chunks"] = []
        return event

    # Allow a KeyError if 'NS' not found.  The step function will abort on
    # this error, as it should, since IDSET_ATTR should be a numeric set.
    ids = resp["Item"][IDSET_ATTR]["NS"]

    start = int(event["worker_id"]) * num_ids_per_worker
    # event['ids'] = ids[start:start+num_ids_per_worker]
    num_chunks = math.ceil(num_ids_per_worker / id_chunk_size)
    total_ids = len(ids)
    event["id_chunks"] = [
        ids[get_chunk_indices(start, id_chunk_size, total_ids, i)]
        for i in range(num_chunks)
    ]

    return event


def read_dynamo(s3_index, obj_key, version):
    """
    Get the ids contained in the cuboid.

    Args:
        s3_index (str): Name of the Dynamo s3_index table.
        obj_key (str): Object key of the cuboid.
        version (str): Cuboid version (reserved for future use).

    Returns:
        (dict): DynamoDB response dictionary.
    """
    client = boto3.client("dynamodb")
    return client.get_item(
        TableName=s3_index,
        Key={"object-key": {"S": obj_key}, "version-node": {"N": version}},
        ProjectionExpression="#idset",
        ExpressionAttributeNames={"#idset": IDSET_ATTR},
        ReturnConsumedCapacity="NONE",
    )


def get_chunk_indices(start, chunk_size, total_num_ids, ind):
    """
    Get slice indices for the nth chunk.

    Args:
        start (int): Base index into the id list.
        chunk_size (int): How many ids go into each chunk.
        total_num_ids (int): Total number of ids in cuboid.
        ind (int): Which chunk to generated indices for.

    Returns:
        (Slice)
    """
    begin = start + chunk_size * ind
    end = min(begin + chunk_size, total_num_ids)

    return slice(begin, end)
