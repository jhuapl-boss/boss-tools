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
#   "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
# }
#
#

import sys
import json
import boto3
import uuid
import logging


# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

#======== for testing locally ================
# from delete_cuboid import *
# event = {
#     "lambda-name": "delete_lambda",
#     "db": "endpoint-db.hiderrt1.boss",
#     "meta-db": "bossmeta.hiderrt1.boss",
#     "s3-index-table": "s3index.hiderrt1.boss",
#     "id-index-table": "idIndex.hiderrt1.boss",
#     "id-count-table": "idCount.hiderrt1.boss",
#     "cuboid_bucket": "cuboids.hiderrt1.boss",
#     "delete_bucket": "delete.hiderrt1.boss",
#     "query-deletes-sfn-name": "QueryDeletesHiderrt1Boss",
#     "delete-sfn-name": "DeleteCuboidHiderrt1Boss",
#     "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
# }
#===========================================


log = logging.getLogger()
log.setLevel(logging.INFO)

sfn_client = boto3.client('stepfunctions')
response = sfn_client.list_state_machines(maxResults=1000)
found_query = False
found_delete = False
while True:
    for sfn in response["stateMachines"]:
        if sfn["name"] == event["query-deletes-sfn-name"]:
            event["query-deletes-sfn-arn"] = sfn["stateMachineArn"]
            found_query = True
        if sfn["name"] == event["delete-sfn-name"]:
            event["delete-sfn-arn"] = sfn["stateMachineArn"]
            found_delete = True
        if found_query and found_delete:
            break
    if found_query and found_delete:
        break
    if 'nextToken' in response:
        response = sfn_client.list_state_machines( maxResults=1000, nextToken=response["nextToken"])
    else:
        break

response = sfn_client.start_execution(
    stateMachineArn=event["query-deletes-sfn-arn"],
    name="delete-boss-{}".format(uuid.uuid4().hex),
    input=json.dumps(event)
)
log.info(response)

