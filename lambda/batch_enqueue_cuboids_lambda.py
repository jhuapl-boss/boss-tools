# Lambda that places up to 10 cuboids in an SQS queue for id indexing via the
# indexing cuboid supervisor step function.
#
# Required inputs:
# {
#   "config": {
#     "object_store_config": {
#       "index_cuboids_keys_queue": "..."
#     }
#   },
#   "cuboid_msgs": [...],
# }
#
# Outputs (all of inputs with following changes/additions):
# {
#    "cuboid_msgs": [...]       # Either empty or only msgs that failed to enqueue.
#    "enqueue_done": ...        # True if all msgs enqueued.
# }
#
# Errors that a step function should abort on:
#   QueueDoesNotExist
#   UnsupportedOperation
#   InvalidAttributeName

import boto3

def handler(event, context):

    queue = event['config']['object_store_config']['index_cuboids_keys_queue']
    msgs = event['cuboid_msgs']

    sqs = boto3.client('sqs')

    resp = sqs.send_message_batch(
        QueueUrl=queue,
        Entries=msgs
    )

    if 'Failed' in resp and len(resp['Failed']) > 0:
        print('send_message_batch() failed: {}'.format(resp))
        failed_msg_ids = [f['Id'] for f in resp['Failed']]
        msgs = [m for m in msgs if m['Id'] in failed_msg_ids]

        # Return only the failed messages.  The step function will have a 
        # choice state that will reinvoke this lambda if event['cuboids_msgs']
        # is not empty..
        event['cuboid_msgs'] = msgs
        event['enqueue_done'] = False

    else:
        # Indicate that all messages were sent.
        event['cuboid_msgs'] = []
        event['enqueue_done'] = True

    return event
