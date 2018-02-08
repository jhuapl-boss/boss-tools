# Lambda that gets the approximate number of messages currently visible in
# the cuboids keys queue (cuboidKeys).
#
# This lambda is executed by the index supervisor step function.  The next
# state is a choice state that starts step functions to dequeue messages from
# the cuboids keys queue and starts indexing the cuboids.
#
# Required inputs:
# {
#   "config": {
#     "object_store_config": {
#       "index_cuboids_keys_queue": "..."
#     }
#   }
# }
#
# Outputs (all of inputs with following changes/additions):
# {
#   "ApproximateNumberOfMessages": ...      # (int)
#   "finished": False                       # For chaining with a choice state used for async fanout.
# }
#
# Errors that a step function should abort on:
#   QueueDoesNotExist
#   UnsupportedOperation
#   InvalidAttributeName

import boto3

class BadSqsResponse(Exception):
    """
    Custom exception to let a step function know that it should retry based on
    a bad response from SQS.
    """
    pass


def handler(event, context):
    queue = event['config']['object_store_config']['index_cuboids_keys_queue']

    sqs = boto3.client('sqs')
    resp = sqs.get_queue_attributes(
        QueueUrl=queue,
        AttributeNames=['ApproximateNumberOfMessages']
    )

    num_msgs = parse_num_msgs_response(event, resp)
    event['ApproximateNumberOfMessages'] = num_msgs

    # Step function's next state will be a choice state that uses this flag
    # to know when to asynchronously fanning out is complete.
    event['finished'] = False
    
    return event


def parse_num_msgs_response(event, resp):
    if 'Attributes' not in resp: 
        raise BadSqsResponse('No Attributes in response')

    if 'ApproximateNumberOfMessages' not in resp['Attributes']:
        raise BadSqsResponse('No ApproximateNumberOfMessages in response')

    return int(resp['Attributes']['ApproximateNumberOfMessages'])
