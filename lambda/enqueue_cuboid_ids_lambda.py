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
import boto3, json, math, time

# TODO: Consider moving these to a common module
SQS_BATCH_SIZE = 10
SQS_RETRY_TIMEOUT = 15
LAMBDA_WAIT_TIME = 60

# lambdas for type checking
_isIterable = lambda x: hasattr(x,'__iter__')
_istype = lambda t: lambda x: type(x) is t
# array of event fields with allowed types and flag to pass through as message field
event_types = [('ids',lambda x: _isIterable(x) and not _istype(str)(x), False, "iterable"), 
               ('num_ids_per_msg',_istype(int), False, "integer"),
               ('attempt', _istype(int), False, "integer"),
               ('sqs_url',_istype(str), True, "string"), 
               ('cuboid_object_key',_istype(str), True, "string"), 
               ('config', _istype(dict), True, "dictionary"), 
               ('id_index_step_fcn',_istype(str), True, "string"),
               ('version',_istype(str), True, "string")]
event_fields = [e[0] for e in event_types]
message_fields = [e[0] for e in event_types if e[2]]

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
            sqs_url (str): the aws url for SQS queue
            cuboid_object_key (str): passed to sqs message
            config (dict): passed to sqs message
            id_index_step_fcn (str): passed to sqs message
            version (str): 

        context (dict): properties of the lambda call e.g. function_name

    Returns:
        If all messages are enqueued, the handler will return an empty dictionary object.
        Otherwise, the dictionary object will contain:
            ids (list):  all of the ids there were not enqueued
            wait_time (int): the number of seconds to wait before retrying the IDs.
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
    invalidTypes = [e for e in event_types if not e[1](event[e[0]])]
    if invalidTypes:
        raise TypeError(";".join([f"Expected {e[0]} as {e[3]}, found {type(event[e[0]])}" for e in invalidTypes]))
    
    # make sure we can access the queue
    sqs = boto3.resource("sqs")
    queue = sqs.Queue(event['sqs_url'])
 
    # create message generator
    msgs = create_messages(event)
    # enqueue the messages
    return enqueue_messages(queue, msgs, event['attempt'])

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

def build_retry_response(batch, msgs, attempt):
    """Build the response message for retrying IDs that were not sent.
    """
    def getNextId(msg):
        m = json.loads(msg)
        for cid in m['id_group']:
            yield cid
    def getId():
        for e in batch:
            for cid in getNextId(e['MessageBody']):
                yield cid
        for m in  msgs:
            for cid in getNextId(m):
                yield cid 
    return { "ids" : [i for i in getId()], "wait_time" : attempt * LAMBDA_WAIT_TIME}

def enqueue_messages(queue, msgs, attempt):
    """Sends messages for id indexing to the queue.

    Args:
        queue (SQS.Queue): The upload queue.
        msgs (Iterator[str]): Stringified JSON messages. 
        attempt (int): The attempt number 

    Returns:
        If all messages are enqueued, an empty dictionary is returned
        If there are failed events, the following fields are included:
            ids (list): A list of IDs that were not sent
            wait_time (int): Number of seconds to wait before retrying
    """
    response = {}

    while not response:
        batch = build_batch_entries(msgs)
        if len(batch) == 0:
            break

        retry = 3
        while retry > 0:
            resp = queue.send_messages(Entries=batch)
            if 'Failed' in resp and len(resp['Failed']) > 0:
                time.sleep(SQS_RETRY_TIMEOUT)
                batch = get_retry_batch(resp['Failed'], batch)
                retry -= 1
                if retry == 0:
                    # populate the response with all remaining IDs
                    return build_retry_response(batch, msgs, attempt)
            else:
                break

    return response

# a generator that produces messages from the event data
def create_messages(event):
    """Create messages from the event data.
    """

    # select the control parameters
    ids = event['ids']
    ids_per_msg = event['num_ids_per_msg']

    # select the constant fields
    base_msg = { f : event[f] for f in message_fields }
    base_msg['id_group'] = []

    # add the block of ids to the base message
    for i in ids:
        base_msg['id_group'].append(i)
        if len(base_msg['id_group']) == ids_per_msg:
            yield json.dumps(base_msg)
            base_msg['id_group'] = []
    if base_msg['id_group']:
        yield json.dumps(base_msg)        
