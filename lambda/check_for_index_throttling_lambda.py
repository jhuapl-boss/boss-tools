# Lambda that checks for write throttling of the id index table in the last
# 60 seconds.
#
# It expects to get from events dictionary
# {
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   'id_index_step_fcn': '...',
# }
#
# Outputs (all of inputs with following changes/additions):
# {
#     "write_throttled": ...     # (bool): True if id index has been write throttled.
#     "read_throttled": ...      # (bool): True if id index has been read throttled.
# }
#
# Errors that a step function should abort on:
#   InvalideParameterValueException
#   InvalideParameterCombinationException
#   InvalideFormatFault
#   MissingRequiredParameterException
#   ResourceNotFound

import boto3
from datetime import datetime, timedelta, timezone

def handler(event, context):
    cw = boto3.client('cloudwatch')
    utc_zone = timezone(timedelta(hours=0))
    now = datetime.now()
    delta = timedelta(minutes=-1)
    id_index_table = event['config']['object_store_config']['id_index_table']

    resp = cw.get_metric_statistics(
        Namespace='AWS/DynamoDB',
        MetricName='WriteThrottleEvents',
        Dimensions=[{'Name': 'TableName', 'Value': id_index_table}],
        StartTime=now + delta,
        EndTime=now,
        Period=60,
        Statistics=['Sum']
    )

    write_throttle_data = parse_throttle_response(event, resp, 'write_throttled')

    resp = cw.get_metric_statistics(
        Namespace='AWS/DynamoDB',
        MetricName='ReadThrottleEvents',
        Dimensions=[{'Name': 'TableName', 'Value': id_index_table}],
        StartTime=now + delta,
        EndTime=now,
        Period=60,
        Statistics=['Sum']
    )

    read_write_throttle_data = parse_throttle_response(
        write_throttle_data, resp, 'read_throttled')

    return read_write_throttle_data


def parse_throttle_response(event, resp, key_name):
    """
    Parse the get_metric_statistics() response.

    Adds the key specified by key_name to the event dictionary.  The value is 
    True if write throttles detected in the last minute.

    Args:
        event (dict): Event dictionary passed to lambda function.
        resp (dict): Response dictionary returned by boto3.CloudWatch.get_metric_statistics() response.
        key_name (str): Name of key to set in event dictionary.

    Returns:
        (dict): The event dictionary with the addition of the specified key.
    """

    # Default to not write throttled.
    event[key_name] = False

    if 'Datapoints' not in resp:
        return event

    num_throttles = len(resp['Datapoints'])
    if num_throttles < 1:
        return event

    print('Detected at least {} {} throttle events.'.format(
        num_throttles, key_name))
    event[key_name] = True
    return event

