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
#   'version': '...',
#   'write_id_index_status': {
#       'done': False,
#       'delay': 0,
#       'retries': 0
#   }
# }

import botocore
from bossutils.aws import get_region
import json
import random
from spdb.spatialdb.object_indices import ObjectIndices
from time import sleep

BASE_DELAY_TIME_SECS = 5

def handler(event, context):
    id_index_table = event['id_index_table']
    s3_index_table = event['s3_index_table']
    id_count_table = event['id_count_table']
    cuboid_bucket = event['cuboid_bucket']

    write_id_index_status = event['write_id_index_status']

    id_index_new_chunk_threshold = (event['id_index_new_chunk_threshold'])

    obj_ind = ObjectIndices(
        s3_index_table, id_index_table, id_count_table, cuboid_bucket, 
        get_region())

    try:
        for obj_id in event['id_group']:
            obj_ind.write_id_index(
                id_index_new_chunk_threshold, 
                event['cuboid_object_key'], obj_id, event['version'])
        write_id_index_status['done'] = True
    except botocore.exceptions.ClientError as ex:
        # Probably had a throttle or a ConditionCheckFailed.
        event['result'] = json.dumps(exception_to_dict(ex))
        print('ClientError caught')
        print(event['result'])
        prep_for_retry(write_id_index_status)

    return event


def prep_for_retry(write_id_index_status):
    """
    Update the given dictionary so the step function knows to retry.

    Args:
        write_id_index_status (dict): Update this dict.
    """
    write_id_index_status['done'] = False
    write_id_index_status['retries'] = (
        int(write_id_index_status['retries']) + 1)

    # Prepare decorrelated jitter backoff delay.
    last_delay = int(write_id_index_status['delay'])
    if last_delay < BASE_DELAY_TIME_SECS:
        last_delay = BASE_DELAY_TIME_SECS
    write_id_index_status['delay'] = round(
        random.uniform(BASE_DELAY_TIME_SECS, last_delay * 3))


def exception_to_dict(ex):
    """
    Convert an exception to a dictionary for JSON serialization.

    Adapted from 
    https://stackoverflow.com/questions/45240549/how-to-serialize-an-exception

    Args:
        ex (Exception): Instance of an exception.

    Returns:
        (dict): Exception prepared for serialization.
    """
    ex_dict = {'type': ex.__class__.__name__}

    if hasattr(ex, 'message'):
        ex_dict['message'] = ex.message

    if hasattr(ex, 'errno'):
        ex_dict['errno'] = ex.errno

    if hasattr(ex, 'strerror'):
        ex['strerror'] = exception_to_dict(ex.strerror) if isinstance(
            ex.strerror, Exception) else ex.strerror

    return ex_dict
