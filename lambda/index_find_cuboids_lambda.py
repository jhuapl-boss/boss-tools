# Lambda that retrieves cuboids from a particular annotation channel in the
# S3 index DynamoDB table.  The table's global secondary index allows this
# retrieval because its primary key is the lookup key.  Note that a channel's
# cuboids are spread across spdb.spatialdb.object.LOOKUP_KEY_MAX_N + 1 keys.
# When the base lookup key is passed in, '#n' is appended to the key where n
# is [0, LOOKUP_KEY_MAX_N], inclusive.
#
# Used by Index.FindCuboids (index_find_cuboids.hsd) step function.
#
# The cuboids' S3 object keys are placed in the dictionary returned by the 
# lambda under the key 'obj_keys'.  The results may be used by the next state
# of a step function.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...',
#   'batch_enqueue_cuboids_step_fcn': '...',  # (string): Passed to fanout state.
#   'fanout_enqueue_cuboids_step_fcn': '...',  # (string): Passed to fanout state.
#   'lookup_key': ...,            # (str): identifies annotation channel.
#   'max_items': ...              # (int): max number of items to retrieve.
#   'exclusive_start_key': ...    # (str): JSON encoded dict representing the last key retrieved.  Empty when there are no items remaining in the index.
#   'status': {
#       'done': bool,
#       'lookup_key_n': ...     # (int): append this to the lookup_key when querying.  Should be zero, initially.
#   }
# }
#
# Output (inputs plus changed/added keys):
# {
#   "finished": bool
#   "obj_keys": [...]
#   "num_batches": Number of batches of obj_keys for enqueuing to SQS
#   "first_time": False
#   "sfn_arn": "..."
# }

import boto3
import json
import math
from spdb.spatialdb.object import LOOKUP_KEY_MAX_N

# Attribute names in S3 index table.
LOOKUP_KEY = 'lookup-key'
OBJ_KEY = 'object-key'
VERSION_NODE = 'version-node'

# Name of global secondary index in S3 index table.
LOOKUP_KEY_INDEX = 'lookup-key-index'

SQS_MAX_BATCH = 10

def handler(event, context):
    table = event['config']['object_store_config']['s3_index_table']
    key = '{}#{}'.format(event['lookup_key'], event['status']['lookup_key_n'])
    limit = int(event['max_items'])

    dynamo = boto3.client('dynamodb')

    query_args = {
        'TableName': table,
        'IndexName': LOOKUP_KEY_INDEX,
        'KeyConditionExpression': '#lookupKey = :lookupKeyVal'.format(LOOKUP_KEY),
        'ExpressionAttributeNames': {
            '#lookupKey': LOOKUP_KEY,
            '#objKey': OBJ_KEY,
            '#versionNode': VERSION_NODE
        },
        'ExpressionAttributeValues': {':lookupKeyVal': {'S': key}},
        'Limit': limit,
        'ProjectionExpression': '#objKey, #versionNode'
    }

    if 'exclusive_start_key' in event and event['exclusive_start_key'] != '':
        query_args['ExclusiveStartKey'] = json.loads(event['exclusive_start_key'])


    resp = dynamo.query(**query_args)

    # This flag controls whether the step function will execute the fanout task..
    event['finished'] = 'Items' not in resp

    if 'Items' in resp:
        keys = resp['Items']
        num_keys = len(keys)
        num_batches = math.ceil(num_keys / SQS_MAX_BATCH)
        event['obj_keys'] = [
            keys[get_batch_indices(SQS_MAX_BATCH, num_keys, i)]
                                   for i in range(num_batches)
        ]
    else:
        event['obj_keys'] = []

    event['num_batches'] = len(event['obj_keys'])

    # This key indicates that there are still cuboids left to retrieve.
    if 'LastEvaluatedKey' in resp:
        event['exclusive_start_key'] = json.dumps(resp['LastEvaluatedKey'])
    else:
        event['exclusive_start_key'] = ''
        n = int(event['status']['lookup_key_n'])
        if n < LOOKUP_KEY_MAX_N:
            event['status']['lookup_key_n'] = n + 1
        else:
            event['status']['done'] = True


    # Give start step function lambda the arn of the step function to run.
    event['sfn_arn'] = event['batch_enqueue_cuboids_step_fcn']

    return event


def get_batch_indices(batch_size, total_num_keys, ind):
    """
    Get slice indices for the nth batch.

    Args:
        batch_size (int): How many ids go into each chunk.
        total_num_keys (int): Total number of ids in cuboid.
        ind (int): Which batch to generated indices for.

    Returns:
        (Slice)
    """
    begin = batch_size * ind
    end = min(begin + batch_size, total_num_keys)

    return slice(begin, end)
