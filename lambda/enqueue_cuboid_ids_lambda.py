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

import boto3
import json
import math

event_fields = ['ids', 'cuboid_object_key', 'config', 'id_index_step_fcn', 'sqs_url', 'num_ids_per_msg']

def handler(event, context=None):
    """Handles the enqueue cuboid ids event.

    This function will create a set of messages and enqueue them in the SQS queue provided in the event. 
    The ids key should contain a list of cuboid IDs and the num_ids_per_msg key is used to distribute
    the cuboid ids among the messages. The cuboid_object_key, config, and id_index_step_fcn keys will be
    passed on in the enqueued message.

    Args:
        event: dict of parameters for the event
            ids: list of str ids for the cuboid
            num_ids_per_msg: maximum number of IDs per message
            sqs_url: str
            cuboid_object_key: 
            config: dict
            id_index_step_fcn: str

        context: dict of properties of the lambda call e.g. function_name

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
        raise ValueError(f"Missing fields: {_}")
    if not type(event['num_ids_per_msg']) is int:
        raise TypeError(f"Expected int for num_ids_per_msg. Found type {type(event['num_ids_per_msg'])}")
    if not type(event['ids']) is list:
        raise TypeError(f"Expected list for ids. Found type {type(event['ids'])}")

    msgs = create_messages(event)
    # enqueue the messages

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
