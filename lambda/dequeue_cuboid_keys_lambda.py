# Lambda that dequeues a batch of messages from the cuboids keys (Cuboidkeys) 
# queue and starts the Index.CuboidSupervisor step function for each key.
#
# Required inputs:
# {
#   "config": {
#     "object_store_config": {
#       "index_cuboids_keys_queue": "..."
#     }
#   },
#   "ApproximateNumberOfMessages": ...      # (int) # msgs in queue.
#   "index_dequeue_cuboids_step_fcn": ...   # (str) Arn of step function.
#   "id_index_step_fcn": ...                # (str) Arn of Index.IdWriter step function.
#   "id_cuboid_supervisor_step_fcn": ...    # (str) Arn of Index.CuboidSupervisor step function.
# }
#
# Outputs: None
#
# Errors that a step function should abort on:
#   QueueDoesNotExist
#   UnsupportedOperation
#   InvalidAttributeName

import boto3
from fanout_dequeue_cuboid_keys_lambda import SQS_MAX_RCV
from hashlib import md5
import json
import start_sfn_lambda

class CorruptSqsResponseError(Exception):
    """
    Indicate that the response from SQS was corrupted.
    """
    pass

def handler(event, context):

    queue = event['config']['object_store_config']['index_cuboids_keys_queue']
    event['sfn_arn']  = event['id_cuboid_supervisor_step_fcn']
    event['operation'] = 'write_s3_index'

    sqs = boto3.client('sqs')
    resp = sqs.receive_message(
        QueueUrl=queue,
        MaxNumberOfMessages=SQS_MAX_RCV,
        WaitTimeSeconds=5
    )

    if 'Messages' not in resp:
        raise CorruptSqsResponseError('SQS response missing Messages key')


    print('Received {} messages.'.format(len(resp['Messages'])))

    # Try to launch Index.CuboidSupervisor step function for each message.
    # If there is a failure, just keep going because the message will become
    # available for processing again, later.
    for msg in resp['Messages']:
        try:
            check_response(msg)
        except CorruptSqsResponseError as ex:
            print(str(ex))
            continue

        try:
            key_version = get_key_and_version(msg)
        except CorruptSqsResponseError as ex:
            print(str(ex))
            continue

        event['cuboid_object_key'] = key_version[0]
        event['version'] = key_version[1]

        try:
            start_sfn_lambda.handler(event, context)
        except:
            print('Failed to start Index.CuboidSupervisor step function')
            continue

        # Index.CuboidSupervisor successfully started, so safe to delete this
        # message from SQS.
        try:
            sqs.delete_message(
                QueueUrl=queue, ReceiptHandle=msg['ReceiptHandle'])
        except:
            print('Failed to delete message {} from queue'.format(
                msg['MessageId']))
            continue


def check_response(msg):
    """
    Make sure message contains all the expected keys and the MD5 hash is good.

    Args:
        msg (dict): A message contained in the response returned by SQS.Client.receive_message().

    Raises:
        (CorruptSqsResponseError): If an expected key is missing from msg or the MD5 does not match.
    """
    if 'MessageId' not in msg:
        raise CorruptSqsResponseError('Message missing MessageId key')

    if 'ReceiptHandle' not in msg:
        raise CorruptSqsResponseError('Message missing ReceiptHandle key')

    if 'Body' not in msg:
        raise CorruptSqsResponseError('Message missing Body key')

    if 'MD5OfBody' not in msg:
        raise CorruptSqsResponseError('Message missing MD5OfBody key')

    if md5(msg['Body'].encode('utf-8')).hexdigest() != msg['MD5OfBody']:
        raise CorruptSqsResponseError('Message corrupt - MD5 mismatch')


def get_key_and_version(msg):
    """
    Extracts object-key and version-node from SQS message body.

    Assumes check_response() has already been run on msg.

    Args:
        msg (dict): A message contained in the response returned by SQS.Client.receive_message().

    Returns:
        (str, str): Tuple with object-key and version-node.

    Raises:
        (CorruptSqsResponseError): If an expected key is missing from msg.
    """
    try:
        body = json.loads(msg['Body'])
    except:
        raise CorruptSqsResponseError('Could not convert body to JSON')

    if 'object-key' not in body:
        raise CorruptSqsResponseError('Message Body missing object-key key')

    if 'version-node' not in body:
        raise CorruptSqsResponseError('Message Body missing version-node key')

    return body['object-key']['S'], body['version-node']['N']

