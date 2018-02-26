# Lambda that loads the list of unique ids belonging to a cuboid and outputs
# a subset of those ids as the value of the 'ids' key.  Because step function
# states limit data passed between them to 32K, the ids are divvied up in 
# this manner.
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
#   'ids_s3_key': ...,          # S3 key where ids are stored for this cuboid.
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

def handler(event, context):
    s3_key = event['ids_s3_key']
    bucket = event['cuboid_ids_bucket']

    client = boto3.client('s3')
    resp = client.get_object(Bucket=bucket, Key=s3_key)
    raw_bytes = resp['Body'].read()
    ids = json.loads(raw_bytes)

    start = int(event['worker_id']) * NUM_IDS_PER_WORKER
    event['ids'] = ids[start:start+NUM_IDS_PER_WORKER]

    event['finished'] = False

    return event

