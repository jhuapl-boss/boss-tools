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

# Objectives
# 1. helper function to create messages
# 2. seperate function to enque the messages in batch
# 3. Failure recovery for any failed messages in the SQS

import boto3
import json
import math

event_fields = ['ids', 'cuboid_object_key', 'config', 'id_index_step_fcn', 'sqs_url', 'num_ids_per_msg']

def handler(event, context):
    """
    Parameters
    ----------
    event : dict 
        parameters for the event, must include all event_fields
            ids : list
            cuboid_object_key : str
            config : dict
            id_index_step_fcn : str
            sqs_url : str
            num_ids_per_msg : int

    context : dict 
        properties of the lambda call e.g. function_name

    Raises
    -------
    ValueError
        Empty event or missing fields
    TypeError
        Expected type for event field not found
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
    """
    Parameters
    ----------
    event : dict
        includes parameters to create the messages

    Returns:
    generator 
        iterator of json encoded strings
    """

    # select the control parameters
    ids = event['ids']
    ids_per_msg = event['num_ids_per_msg']

    # select the constant fields
    base_fields = [f for f in event_fields if f not in ['ids','num_ids_per_msg'] ]
    base_msg = { f : event[f] for f in base_fields }

    ngroups = int(math.ceil(len(ids)/ids_per_msg))
    # add the block of ids to the base message
    for i in range(ngroups):
        offset = ids_per_msg*i
        base_msg['id_group'] = ids[offset:offset+ids_per_msg]
        yield json.dumps(base_msg)         
