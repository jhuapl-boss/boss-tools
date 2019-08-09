# Lambda to write the morton index of a cuboid object key to the id in the
# DynamoDB id index table.
#
# If there are failures, uses decorrelatd jitter backoff algorithm described in:
# https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
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
#       'retries_left': int     # How many retries left in case of an error.
#   }
# }

import botocore
from bossutils.aws import get_region
import json
import random
from spdb.spatialdb.object_indices import ObjectIndices
from time import sleep

BASE_DELAY_TIME_SECS = 5

"""
These derived exceptions of botocore.exceptions.ClientError will be not be 
retried by the step function that calls this lambda.  Since this lambda 
controls retries via event['write_id_index_status']['retries_left'] the step 
function should proceed to its catch handling when it receives one of these
exceptions.

Derived exceptions of ClientError that are not part of this list get wrapped
in DynamoClientError to ensure the step function goes to its catch handling
step.

The error information available to the step function isn't as useful when
wrapped, so the expected errors are enumerated below and in the step function's
retry statement.
"""
DO_NOT_WRAP_THESE_EXCEPTIONS = [
    'ClientError', 
    'ConditionalCheckFailedException',
    'GlobalTableNotFoundException',
    'InternalServerError',
    'ItemCollectionSizeLimitExceededException',
    'LimitExceededException',
    'ProvisionedThroughputExceededException',
    'ReplicaAlreadyExistsException',
    'ReplicaNotFoundException',
    'ResourceInUseException',
    'ResourceNotFoundException',
    'TableNotFoundException'
]

class DynamoClientError(Exception):
    """
    Wrap boto3 ClientError exceptions so the step function can fail when
    event['write_id_index_status']['retries_left'] == 0.
    """
    def __init__(self, message):
        super().__init__(message)


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
        print('ClientError caught: {}'.format(ex))
        if int(write_id_index_status['retries_left']) < 1:
            if get_class_name(ex.__class__) in DO_NOT_WRAP_THESE_EXCEPTIONS:
                raise
            msg = '{}: {}'.format(type(ex), ex)
            raise DynamoClientError(msg) from ex
        event['result'] = str(ex)
        prep_for_retry(write_id_index_status)

    return event


def prep_for_retry(write_id_index_status):
    """
    Update the given dictionary so the step function knows to retry.

    Args:
        write_id_index_status (dict): Update this dict.
    """
    write_id_index_status['done'] = False
    write_id_index_status['retries_left'] = (
        int(write_id_index_status['retries_left']) - 1)

    # Prepare decorrelated jitter backoff delay.
    last_delay = int(write_id_index_status['delay'])
    if last_delay < BASE_DELAY_TIME_SECS:
        last_delay = BASE_DELAY_TIME_SECS
    write_id_index_status['delay'] = round(
        random.uniform(BASE_DELAY_TIME_SECS, last_delay * 3))


def get_class_name(type_):
    """
    Get just the class name (w/o module(s) from the type.

    Args:
        type_ (type): Class as a type.

    Returns:
        (str|None): Just the name of the class or None.
    """
    try:
        return str(type_).rsplit('.', 1)[1].rstrip("'>")
    except IndexError:
        return None
