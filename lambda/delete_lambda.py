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
#   "delete-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCuboidHiderrt1Boss",
#   "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList"
# }
#
#

import sys
import json
import time
import boto3
import botocore
import uuid
import pprint
import pymysql.cursors
import logging
import bossutils
from datetime import datetime, timedelta


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

DELETED_STATUS_START = 'start'
DELETED_STATUS_ERROR = 'error'
DELETED_STATUS_FINISHED = 'finished'

rds_client = boto3.client('rds')
sfn_client = boto3.client('stepfunctions')

# TODO SH need to get vault working from lambda or pass credentials into lamba.
vault = bossutils.vault.Vault()
name = vault.read('secret/endpoint/django/db', 'name')
user = vault.read('secret/endpoint/django/db', 'user')
password = vault.read('secret/endpoint/django/db', 'password')
port = vault.read('secret/endpoint/django/db', 'port')

#### for testing locally
# event["db_name"] = 'boss'
# event["db_user"] = "testuser"
# event["db_password"] = "xxxxxxxxxxxxx"
# event["db"] = "localhost"
# event["db_port"] = 3306
#####

# Connect to the database
connection = pymysql.connect(host=event["db"],
                             user=event["db_user"],
                             password=event["db_password"],
                             db=event["db_name"],
                             port=int(event["db_port"]),
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
try:
    with connection.cursor() as cursor:
        # Read a single record
        one_day = timedelta(days=1)
        # this query will find 1 item to be deleted that does not have a delete_status (good for debugging)
        sql = "SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `channel` where `to_be_deleted` is not null AND deleted_status is null limit 1"
        cursor.execute(sql)
        # # This query version will only find items older than a day.
        # sql = "SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `channel` where `to_be_deleted` > %s AND deleted_status is null"
        # cursor.execute(sql, (one_day,))
        for ch_row in cursor:
            log.info("found channel: " + str(ch_row))
            print("found channel: " + str(ch_row))

            # get lookup key given channel id and channel name
            channel_id = ch_row['id']
            sql = "SELECT `id`, `channel_name`, `lookup_key` FROM `lookup` where `channel_name`=%s"
            cursor.execute(sql, (ch_row['name'],))
            lookup_key_id = None
            lookup_key = None
            for lookup_row in cursor:  # its possible for two channels to have the same name so we search for the one with the correct channel id.
                parts = lookup_row['lookup_key'].split("&")
                if int(parts[2]) == channel_id:
                    lookup_key_id = lookup_row['id']
                    lookup_key = lookup_row['lookup_key']
                    break
            if lookup_key_id is None:
                log.warning('channel_id {} did not have an associated lookup_key in the lookup'.format(channel_id))
                client = boto3.client('sns')
                resp = client.publish(TopicArn=event["notify_topic"],
                                      Message="Delete Error: channel id {}, has no lookup key in the endpoint lookup table.".format(channel_id))
                if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    log.error("Unable to send to following error to SNS Topic. Received the following HTTPStatusCode {}"
                              .format(resp["ResponseMetadata"]["HTTPStatusCode"]) +
                              "Delete Error: channel id {}, has no lookup key in the endpoint lookup table.".format(channel_id))
                sql = "UPDATE channel SET deleted_status=%s WHERE `id`=%s"
                cursor.execute(sql, (DELETED_STATUS_ERROR, str(channel_id),))
                connection.commit()
            else:
                sql = "UPDATE channel SET deleted_status=%s WHERE `id`=%s"
                cursor.execute(sql, (DELETED_STATUS_START, str(channel_id),))
                connection.commit()
                # Kick off step-function
                event["lookup_key"] = lookup_key
                event["lookup_key_id"] = lookup_key_id
                event["channel_id"] = channel_id

                response = sfn_client.start_execution(
                    stateMachineArn=event["delete-sfn-arn"],
                    name="delete-boss-{}".format(uuid.uuid4().hex),
                    input=json.dumps(event)
                )
                log.info(response)

finally:
    connection.close()



# session = boto3.session.Session(region_name="us-east-1")
# dict = delete_metadata(event, session=session)
# dict = delete_id_count(dict, session=session)
# dict = delete_id_index(dict, session=session)
# dict = find_s3_index(dict, session=session)
# dict = delete_s3_index(dict, session=session)
# dict = delete_clean_up(dict, session=session)
# #dict = notify_admins(input_from_main, session=session)


