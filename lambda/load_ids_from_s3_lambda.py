# Lambda that loads the list of unique ids belonging to a cuboid and outputs
# a subset of those ids as the value of the 'ids' key.  
# 
# Ids are loaded from the Dynamo table: s3index.domain.tld.
#
# Because step function states limit data passed between them to 32K, the ids 
# are divvied up in this manner.
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
#   'fanout_id_writers_step_fcn': '...',    # step func arn
#   'cuboid_object_key': '...',
#   'version': '...',
#   'max_write_id_index_lambdas': int,
#   'worker_id': int            # Id used to index into JSON list of ids.
# }
#
# Outputs (modified or added):
# {
#   'ids': [...]                # List of ids to update in id index table.
#   'finished': False
# }

import boto3
import json
from split_cuboids_lambda import NUM_IDS_PER_WORKER

IDSET_ATTR = 'id-set'

def handler(event, context):
    obj_key = event['cuboid_object_key']
    s3_index = event['config']['object_store_config']['s3_index_table']
    version = str(event['version'])

    client = boto3.client('dynamodb')
    resp = client.get_item(
        TableName=s3_index,
        Key={
            'object-key': {'S': obj_key}, 
            'version-node': {'N': version}
        },
        ProjectionExpression='#idset',
        ExpressionAttributeNames={'#idset': IDSET_ATTR},
        ReturnConsumedCapacity='NONE'
    )

    if 'Item' not in resp or IDSET_ATTR not in resp['Item']:
        # No ids found, so nothing to do.
        event['ids'] = []
        event['finished'] = True
        return event

    # Allow a KeyError if 'NS' not found.  The step function will abort on 
    # this error, as it should, since IDSET_ATTR should be a numeric set.
    ids = resp['Item'][IDSET_ATTR]['NS']

    start = int(event['worker_id']) * NUM_IDS_PER_WORKER
    event['ids'] = ids[start:start+NUM_IDS_PER_WORKER]
    event['finished'] = False

    return event
