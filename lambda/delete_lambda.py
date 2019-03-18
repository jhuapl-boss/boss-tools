#!/usr/bin/env python3.4
# This lambda calls the query-deletes-sfn.
# in the process it will lookup the ARNs for query-deletes-sfn and delete-sfn-name and add them to the
# data returned.
#
# It expects to get from events dictionary
# {
#   "lambda-name": "delete_lambda",
#   "db": "endpoint-db.hiderrt1.boss",
#   "meta-db": "bossmeta.hiderrt1.boss",
#   "s3-index-table": "s3index.hiderrt1.boss",
#   "id-index-table": "idIndex.hiderrt1.boss",
#   "id-count-table": "idCount.hiderrt1.boss",
#   "cuboid_bucket": "cuboids.hiderrt1.boss",
#   "delete_bucket": "delete.hiderrt1.boss"
#   "query-deletes-sfn-name": "QueryDeletesHiderrt1Boss"
#   "delete-sfn-name": "DeleteCuboidHiderrt1Boss",
#   "delete-exp-sfn-name": "DeleteExperimentHiderrt1Boss",
#   "delete-coord-frame-sfn-name": "DeleteCoordframeHiderrt1Boss",
#   "delete-coll-sfn-name": "DeleteCollectionHiderrt1Boss",
#   "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
# }
#
#

import sys
import json
import boto3
import uuid
import logging

def got_all_step_funcs(event, debug=False):
    """
    Check if all step function arns found.

    Args:
        event (dict): Step functions arns are set as they are found.
        debug (bool): Print debug messages.

    Returns:
        (bool): True if all step functions found.
    """
    if event['delete-sfn-arn'] is None:
        if debug:
            print('No delete-sfn')
        return False

    if event['query-deletes-sfn-arn'] is None:
        if debug:
            print('No query-deletes-sfn')
        return False

    if event["delete-exp-sfn-arn"] is None:
        if debug:
            print('No delete-exp-sfn')
        return False

    if event["delete-coord-frame-sfn-arn"] is None:
        if debug:
            print('No delete-coord-frame-sfn')
        return False

    if event["delete-coll-sfn-arn"] is None:
        if debug:
            print('No delete-coll-sfn')
        return False

    return True


# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

# Initialize new keys for event dict.  These will be populated after getting
# the list of all state machines.
event['delete-sfn-arn'] = None
event['query-deletes-sfn-arn'] = None
event["delete-exp-sfn-arn"] = None
event["delete-coord-frame-sfn-arn"] = None
event["delete-coll-sfn-arn"] = None

#======== for testing locally ================
event["lambda-name"] = "delete_lambda"
event["db"] = "endpoint-db.giontc1.boss"
event["meta-db"] = "bossmeta.giontc1.boss"
event["s3-index-table"] = "s3index.giontc1.boss"
event["id-index-table"] = "idIndex.giontc1.boss"
event["id-count-table"] = "idCount.giontc1.boss"
event["cuboid_bucket"] = "cuboids.giontc1.boss"
event["delete_bucket"] = "delete.giontc1.boss"
event["query-deletes-sfn-name"] = "QueryDeletesGiontc1Boss"
event["delete-sfn-name"] = "DeleteCuboidGiontc1Boss"
event["delete-exp-sfn-name"] = "DeleteExperimentGiontc1Boss"
event["delete-coord-frame-sfn-name"] = "DeleteCoordframeGiontc1Boss"
event["delete-coll-sfn-name"] = "DeleteCollectionGiontc1Boss"
event["topic-arn"] = "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
#==========================================


log = logging.getLogger()
log.setLevel(logging.INFO)

sfn_client = boto3.client('stepfunctions')
response = sfn_client.list_state_machines(maxResults=1000)
found_all = False
while True:
    for sfn in response["stateMachines"]:
        if sfn["name"] == event["query-deletes-sfn-name"]:
            event["query-deletes-sfn-arn"] = sfn["stateMachineArn"]
        elif sfn["name"] == event["delete-sfn-name"]:
            event["delete-sfn-arn"] = sfn["stateMachineArn"]
        elif sfn["name"] == event["delete-exp-sfn-name"]:
            event["delete-exp-sfn-arn"] = sfn["stateMachineArn"]
        elif sfn["name"] == event["delete-coord-frame-sfn-name"]:
            event["delete-coord-frame-sfn-arn"] = sfn["stateMachineArn"]
        elif sfn["name"] == event["delete-coll-sfn-name"]:
            event["delete-coll-sfn-arn"] = sfn["stateMachineArn"]
        if got_all_step_funcs(event, True):
            found_all = True
            break
    if found_all:
        break
    if 'nextToken' in response:
        response = sfn_client.list_state_machines( maxResults=1000, nextToken=response["nextToken"])
    else:
        break

if not found_all:
    log.error('Did not find all required step functions for delete!')

response = sfn_client.start_execution(
    stateMachineArn=event["query-deletes-sfn-arn"],
    name="delete-boss-{}".format(uuid.uuid4().hex),
    input=json.dumps(event)
)
log.info(response)

