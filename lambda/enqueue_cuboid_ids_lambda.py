# Copyright 2021 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Enueques cuboid IDs for processing by a step function.

   This lambda will distribute the provided cuboid IDs into multiple SQS messages and enqueue them.

   Usage:
        A caller will pass an event object that is consumed by the handler function. The handler
        will return any Ids that are not enqueued and a delay in seconds for the retry.
"""

from typing import Iterable
from collections import namedtuple
import boto3, json, os, random

# TODO: Consider moving these to a common module
SQS_BATCH_SIZE = 10
SQS_RETRY_TIMEOUT = 15
LAMBDA_WAIT_TIME = 60

"""
Defines expected keys in the events dictionary passed to the lambda handler.

Args:
    name (str): Name of key in event dictionary.
    type_guard_fcn (function): Returns true if the key's value is the correct type.
    include_in_msgs (bool): If true, include this key in the SQS message.
    type_str (str): Friendly string that identifies the type expected for the key's value.
    rename_to (str|None): If not None and include_in_msgs, the key will be stored in the SQS message with this name.
"""
EventTypes = namedtuple('EventTypes', ['name', 'type_guard_fcn', 'include_in_msgs', 'type_str', 'rename_to'], defaults=(None,))

# lambdas for type checking
_isIterable = lambda x: hasattr(x,'__iter__')
_istype = lambda t: lambda x: type(x) is t
_or = lambda t1, t2: lambda x: t1(x) or t2(x)
# array of event fields with allowed types and flag to pass through as message field
event_types = [EventTypes('ids',lambda x: _isIterable(x) and not _istype(str)(x), False, "iterable"), 
               EventTypes('num_ids_per_msg',_istype(int), True, "integer"),
               EventTypes('attempt', _istype(int), False, "integer"),
               EventTypes('done', _istype(bool), False, "bool"),
               EventTypes('wait_time', _istype(int), False, "integer"),
               EventTypes('sqs_url',_istype(str), True, "string"), 
               EventTypes('cuboid_object_key',_istype(str), True, "string"), 
               EventTypes('config', _istype(dict), True, "dictionary"), 
               EventTypes('id_index_step_fcn',_istype(str), True, "string"),
               EventTypes('version',_or(_istype(str), _istype(int)), True, "string or integer")]
event_fields = [e.name for e in event_types]
message_fields = [e for e in event_types if e.include_in_msgs]

def handler(event, context=None):
    """Handles the enqueue cuboid ids event.

    This function will create a set of messages and enqueue them in the SQS queue provided in the event. 
    The ids key should contain a list of cuboid IDs and the num_ids_per_msg key is used to distribute
    the cuboid ids among the messages. The cuboid_object_key, config, and id_index_step_fcn keys will be
    passed on in the enqueued message.

    Args:
        event (dict): parameters for the event
            ids (iterable): ids for the cuboid
            num_ids_per_msg (int): maximum number of IDs per message
            attempt (int): used to adjust the backoff delay
            done (bool): Should be False. Set to True when all IDs are enqueued
            wait_time (int): the last wait time used
            sqs_url (str): the aws url for SQS queue
            cuboid_object_key (str): passed to sqs message
            config (dict): passed to sqs message
            id_index_step_fcn (str): passed to sqs message
            version (str): passed to sqs message

        context (dict): properties of the lambda call e.g. function_name

    Returns:
        The updated event dictionary:
            ids (iterable): any IDs that were not enqueued due to timeout
            done (bool): True if all IDs were enqueued
            wait_time (int): Set to number of seconds to wait before retrying


    Raises:
        ValueError: Missing or empty event
        KeyError: Missing keys in the event 
        TypeError: Expected type for event field not found
    """

    if not event:
        raise ValueError("Missing or empty event")

    # check for missing fields, 
    missingFields = [k for k in event_fields if k not in event] 
    if missingFields:
        _ = ",".join(missingFields)
        raise KeyError(f"Missing keys: {_}")
    invalidTypes = [e for e in event_types if not e.type_guard_fcn(event[e.name])]
    if invalidTypes:
        raise TypeError(";".join([f"Expected {e.name} as {e.type_str}, found {type(event[e.name])}" for e in invalidTypes]))
    
    # Populate orig_wait_time field if not provided.  wait_time will be mutated
    # on failures, so retain the original value in this new field.  This value
    # will be included in the SQS message so that downstream step functions
    # have access to the original value.
    if 'orig_wait_time' not in event:
        event['orig_wait_time'] = event['wait_time']

    # make sure we can access the queue
    endpoint = os.getenv('SQS_BACKEND', None)
    sqs = boto3.resource("sqs", endpoint_url=endpoint)
    queue = sqs.Queue(event['sqs_url'])
 
    # create message generator
    msgs = create_messages(event)
    # enqueue the messages
    return enqueue_messages(queue, msgs, event)

def get_retry_batch(failed_entries, batch):
    """Return a new batch with failed entries.
    """
    failed_entry_ids = [f['Id'] for f in failed_entries]
    # rebuild the batch using the failed entries
    return [b for b in batch if b['Id'] in failed_entry_ids]

def build_batch_entries(msgs):
    """Get the next batch of messages to enqueue
    """
    batch = []
    for i in range(SQS_BATCH_SIZE):
        try:
            batch.append({
                'Id': str(i),
                'MessageBody': next(msgs),
                'DelaySeconds': 0
            })
        except StopIteration:
            break
    return batch

def build_retry_response(batch, msgs, event):
    """Build the response message for retrying IDs that were not sent.
    """
    def getNextId(msg):
        m = json.loads(msg)
        for cid in m['ids']:
            yield cid
    def getId():
        for e in batch:
            for cid in getNextId(e['MessageBody']):
                yield cid
        for m in  msgs:
            for cid in getNextId(m):
                yield cid 
    event['ids'] = [i for i in getId()]
    event['done'] = len(event['ids']) == 0
    if event['ids']:
        attempt = int(event['attempt']) + 1
        event['attempt'] = attempt
        last_wait_time = int(event['wait_time'])
        if last_wait_time < LAMBDA_WAIT_TIME:
            last_wait_time = LAMBDA_WAIT_TIME
        event['wait_time'] = round(random.uniform(LAMBDA_WAIT_TIME, last_wait_time*attempt))
    
    return event

def enqueue_messages(queue, msgs, event):
    """Sends messages for id indexing to the queue.

    Args:
        queue (SQS.Queue): The upload queue.
        msgs (Iterator[str]): Stringified JSON messages. 
        attempt (int): The attempt number 

    Returns:
        A dictionary with the following fields:
            ids (list): A list of IDs that were not sent
            wait_time (int): Number of seconds to wait before retrying
    """
    
    while True:
        batch = build_batch_entries(msgs)
        if len(batch) == 0:
            break

        retry = 1
        while retry > 0:
            resp = queue.send_messages(Entries=batch)
            if 'Failed' in resp and len(resp['Failed']) > 0:
                batch = get_retry_batch(resp['Failed'], batch)
                retry -= 1
                if retry == 0:
                    # populate the response with all remaining IDs
                    return build_retry_response(batch, msgs, event)
            else:
                break
        event['ids'] = []
        event['done'] = True
        event['wait_time'] = 0
    return event

def get_message_name(field):
    """
    Returns the name to use for the message field.

    Args:
        field (EventTypes)

    Returns:
        (str): Value of name or rename_to if not None.
    """
    if field.rename_to is not None:
        return field.rename_to
    return field.name

# a generator that produces messages from the event data
def create_messages(event):
    """Create messages from the event data.
    """

    # select the control parameters
    ids = event['ids']
    ids_per_msg = event['num_ids_per_msg']

    # select the constant fields
    base_msg = { get_message_name(f) : event[f.name] for f in message_fields }
    base_msg['wait_time'] = event['orig_wait_time']
    # The lambda connected to the SQS queue expects 'sfn_arn', but we also
    # need to keep 'id_index_step_fcn' for downstream invocations of this
    # lambda.
    base_msg['sfn_arn'] = event['id_index_step_fcn']
    base_msg['ids'] = []

    # add the block of ids to the base message
    for i in ids:
        base_msg['ids'].append(i)
        if len(base_msg['ids']) == ids_per_msg:
            yield json.dumps(base_msg)
            base_msg['ids'] = []
    if base_msg['ids']:
        yield json.dumps(base_msg)        
