#!/usr/bin/env python3.4
# This lambda is for queries for Channels, Experiments and Collections that are flagged for deleting
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
#   "find-deletes-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:FindDeletesHiderrt1Boss",
#   "delete-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCuboidHiderrt1Boss",
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
#     "lookup_key": "36&26&31",
#     "lambda-name": "delete_lambda",
#     "db": "endpoint-db.hiderrt1.boss",
#     "meta-db": "bossmeta.hiderrt1.boss",
#     "s3-index-table": "s3index.hiderrt1.boss",
#     "id-index-table": "idIndex.hiderrt1.boss",
#     "id-count-table": "idCount.hiderrt1.boss",
#     "cuboid_bucket": "cuboids.hiderrt1.boss",
#     "delete_bucket": "delete.hiderrt1.boss",
#     "delete-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCuboidHiderrt1Boss",
#     "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
# }
#===========================================


log = logging.getLogger()
log.setLevel(logging.INFO)

sfn_client = boto3.client('stepfunctions')

response = sfn_client.start_execution(
    stateMachineArn=event["delete-sfn-arn"],
    name="delete-boss-{}".format(uuid.uuid4().hex),
    input=json.dumps(event)
)
log.info(response)


# session = boto3.session.Session(region_name="us-east-1")
# dict = delete_metadata(event, session=session)
# dict = delete_id_count(dict, session=session)
# dict = delete_id_index(dict, session=session)
# dict = find_s3_index(dict, session=session)
# dict = delete_s3_index(dict, session=session)
# dict = delete_clean_up(dict, session=session)
# #dict = notify_admins(input_from_main, session=session)


