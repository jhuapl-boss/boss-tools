# Lambda that sends results from a failed index writer lambda executed by a
# step function to the deadletter queue.  The message body should contain the
# required data to rerun the step function.
#
# It expects to get from events dictionary
# {
#   'operation': '...',         # This may be a string or a list.
#   'index_deadletter_queue': ...
#           OR
#   'config': {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
# }

import boto3
import json

def handler(event, context):
    sqs = boto3.client('sqs')

    msg_args = {
        'MessageBody': json.dumps(event),
        'MessageAttributes': {'operation': {'DataType': 'String'}}
    }

    if 'index_deadletter_queue' in event:
        msg_args['QueueUrl'] = event['index_deadletter_queue']
    else:
        msg_args['QueueUrl'] = event['config']['object_store_config']['index_deadletter_queue']

    msg_args['MessageAttributes']['operation']['StringValue'] = get_operation(event)

    sqs.send_message(**msg_args)


def get_operation(event):
    """
    Get the operation from the event data.

    If operation exists in the event data, it may be either a string or list.
    If it is a list, only the first element will be returned.

    Args:
        event (dict): Event data as passed to the lambda.

    Returns:
        (str): 'unknown' if key not found or if given an empty list.
    """

    if 'operation' not in event:
        return 'unknown'

    op = event['operation']
    if isinstance(op, str):
        return op
    elif isinstance(op, list) and len(op) > 0:
        return op[0]

    return 'unknown'
