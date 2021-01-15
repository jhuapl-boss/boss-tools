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
   There
"""

import boto3, json, math, time

# TODO: Consider moving these to a common module
SQS_BATCH_SIZE = 10
SQS_RETRY_TIMEOUT = 15

event_types = [('ids',[list,range]), ('cuboid_object_key',[str]), ('config', [dict]), ('id_index_step_fcn',[str]), ('sqs_url',[str]), ('num_ids_per_msg',[int])]
event_fields = [e[0] for e in event_types]

def handler(event, context=None):
    """Handles the enqueue cuboid ids event.

    This function will create a set of messages and enqueue them in the SQS queue provided in the event. 
    The ids key should contain a list of cuboid IDs and the num_ids_per_msg key is used to distribute
    the cuboid ids among the messages. The cuboid_object_key, config, and id_index_step_fcn keys will be
    passed on in the enqueued message.

    Args:
        event (dict): parameters for the event
            ids (list): ids for the cuboid
            num_ids_per_msg (int): maximum number of IDs per message
            sqs_url (str): the aws url for SQS queue
            cuboid_object_key (str): passed to sqs message
            config (dict): passed to sqs message
            id_index_step_fcn (str): passed to sqs message

        context (dict): properties of the lambda call e.g. function_name

    Raises:
        ValueError: Missing fields in the event
        TypeError: Expected type for event field not found
    """

    if not event:
        raise ValueError("Missing event data")

    # check for missing fields, 
    missingFields = [k for k in event_fields if k not in event] 
    if missingFields:
        _ = ",".join(missingFields)
        raise KeyError(f"Missing keys: {_}")
    invalidTypes = [e for e in event_types if not type(event[e[0]]) in e[1]]
    if invalidTypes:
        raise TypeError(";".join([f"Expected {e[0]}:{e[1]}, found {type(event[e[0]])}" for e in invalidTypes]))
    
    # make sure we can access the queue
    sqs = boto3.resource("sqs")
    queue = sqs.Queue(event['sqs_url'])
 
    # create message generator
    msgs = create_messages(event)
    # enqueue the messages
    enqueue_messages(queue, msgs)

def enqueue_messages(queue, msgs):
    """Sends messages for id indexing to the queue.

    Args:
        queue (SQS.Queue): The upload queue.
        msgs (Iterator[str]): Stringified JSON messages. 

    Returns:
        (bool): True if at least one message was enqueued.
    """
    enqueued_msgs = False

    while True:
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

        if len(batch) == 0:
            break

        retry = 3
        while retry > 0:
            resp = queue.send_messages(Entries=batch)
            if 'Failed' in resp and len(resp['Failed']) > 0:
                time.sleep(SQS_RETRY_TIMEOUT)

                ids = [f['Id'] for f in resp['Failed']]
                batch = [b for b in batch if b['Id'] in ids]
                retry -= 1
                if retry == 0:
                    # what should happen here?
#                    log.error('Could not send {}/{} messages to queue {}'.format(
#                        len(resp['Failed']), len(batch), queue.url))
                    break
            else:
                enqueued_msgs = True
                break

    return enqueued_msgs


# a generator that produces messages from the event data
def create_messages(event):
    """Create messages from the event data.
    """

    # select the control parameters
    ids = event['ids']
    ids_per_msg = event['num_ids_per_msg']

    # select the constant fields
    base_fields = [f for f in event_fields if f not in ['ids','num_ids_per_msg'] ]
    base_msg = { f : event[f] for f in base_fields }
    base_msg['id_group'] = []

    # add the block of ids to the base message
    for i in ids:
        base_msg['id_group'].append(i)
        if len(base_msg['id_group']) == ids_per_msg:
            yield json.dumps(base_msg)
            base_msg['id_group'] = []
    if base_msg['id_group']:
        yield json.dumps(base_msg)        
