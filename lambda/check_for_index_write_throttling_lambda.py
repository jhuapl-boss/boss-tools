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
# }
#
# Errors that a step function should abort on:
#   InvalideParameterValueException
#   InvalideParameterCombinationException
#   InvalideFormatFault
#   MissingRequiredParameterException
#   ResourceNotFound

import boto3
from datetime import datetime, timedelta

def handler(event, context):
    cw = boto3.client('cloudwatch')
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

    return parse_response(event, resp)


def parse_response(event, resp):
    """
    Parse the get_metric_statistics() response.

    Adds the key 'write_throttled' to the event dictionary.  The value is True
    if write throttles detected in the last minute.

    Args:
        event (dict): Event dictionary passed to lambda function.
        resp (dict): Response dictionary returned by boto3.CloudWatch.get_metric_statistics() response.

    Returns:
        (dict): The event dictionary with the addition of the 'write_throttled' key.
    """

    # Default to not write throttled.
    event['write_throttled'] = False

    if 'Datapoints' not in resp:
        return event

    num_throttles = len(resp['Datapoints'])
    if num_throttles < 1:
        return event

    print('Detected at least {} write throttle events.'.format(num_throttles))
    event['write_throttled'] = True
    return event

