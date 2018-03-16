# Lambda that retrieves cuboids from a particular annotation channel in the
# S3 index DynamoDB table.  The table's global secondary index allows this
# retrieval because its primary key is the lookup key.
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
#   'exclusive_start_key': ...    # (str): JSON encoded dict representing the last key retrieved.  Empty on the initial call or when there are no items remaining in the index.
# }
#
# Output (inputs plus changed/added keys):
# {
#   "finished": bool
#   "obj_keys": [...]
#   "first_time": False
#   "sfn_arn": "..."
# }

import boto3
import json

# Attribute names in S3 index table.
LOOKUP_KEY = 'lookup-key'
OBJ_KEY = 'object-key'
VERSION_NODE = 'version-node'

# Name of global secondary index in S3 index table.
LOOKUP_KEY_INDEX = 'lookup-key-index'

def handler(event, context):
    table = event['config']['object_store_config']['s3_index_table']
    key = event['lookup_key']
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
        query_args['ExclusiveStartKey' ] = json.loads(event['exclusive_start_key'])


    resp = dynamo.query(**query_args)

    # This flag controls whether the step function will execute the fanout task..
    event['finished'] = 'Items' not in resp

    if 'Items' in resp:
        event['obj_keys'] = resp['Items']
    else:
        event['obj_keys'] = []

    # This key indicates that there are still cuboids left to retrieve.
    if 'LastEvaluatedKey' in resp:
        event['exclusive_start_key'] = json.dumps(resp['LastEvaluatedKey'])
    else:
        event['exclusive_start_key'] = ''

    # Tell step function that this task state/lambda has executed at least once.
    event['first_time'] = False

    # Give start step function lambda the arn of the step function to run.
    event['sfn_arn'] = event['fanout_enqueue_cuboids_step_fcn']

    return event

