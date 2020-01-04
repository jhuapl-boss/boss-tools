#!/usr/bin/env python3
# Copyright 2017 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This file holds the functions need to perform deletes for Collections, Experiment, Channel
These may be run in Lambdas or Activities in Step Functions.

The entire set of step functions need the following in data
data = {
    "lambda-name": "delete_lambda",
    "db": "endpoint-db.hiderrt1.boss",
    "meta-db": "bossmeta.hiderrt1.boss",
    "s3-index-table": "s3index.hiderrt1.boss",
    "id-index-table": "idIndex.hiderrt1.boss",
    "id-count-table": "idCount.hiderrt1.boss",
    "cuboid_bucket": "cuboids.hiderrt1.boss",
    "delete_bucket": "delete.hiderrt1.boss",
    "find-deletes-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:FindDeletesHiderrt1Boss",
    "delete-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCuboidHiderrt1Boss",
    "delete-exp-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteExperimentHiderrt1Boss",
    "delete-coord-frame-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCoordframeHiderrt1Boss",
    "delete-coll-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:DeleteCollectionHiderrt1Boss",
    "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList",
}

"""
import boto3
import bossutils
from collections import namedtuple
import hashlib
import json
import pymysql.cursors
import uuid
import pprint
from spdb.spatialdb.object import INGEST_ID_MAX_N, AWSObjectStore
from datetime import datetime, timedelta

bossutils.utils.set_excepthook()
LOG = bossutils.logger.bossLogger()
S3_INDEX_TABLE_INDEX = 'ingest-job-index'
INGEST_ID_INDEX = 'ingest-id-index'
MAX_ITEMS_PER_SHARD = 100

# Use this value for cuboids added via cutouts (instead of ingests).
CUTOUT_JOB_ID = 0

DELETED_STATUS_FINISHED = 'finished'
DELETED_STATUS_START = 'start'
DELETED_STATUS_ERROR = 'error'

DATA_FOUND = 'data_found'


class DeleteError(Exception):
    """
    DeleteError will be used if an Exception occurs within any of the delete functions.
    """
    pass


"""Resolution and id of an ingest job."""
ResJobId = namedtuple('ResJobId', ['res', 'job_id'])

def get_db_connection(data):
    """
    connects to vault to get db information and then makes a pymysql connection
    Args:
        data(dict): dictionary containing db key.

    Returns:
        (pymysql.Connection) connection to pymysql
    """
    vault = bossutils.vault.Vault()
    LOG.debug("get_db_connection(): made connection to Vault")

    # ------ get values from Vault -----------
    host = data["db"]
    user = vault.read('secret/endpoint/django/db', 'user')
    password = vault.read('secret/endpoint/django/db', 'password')
    db_name = vault.read('secret/endpoint/django/db', 'name')
    port = int(vault.read('secret/endpoint/django/db', 'port'))

    # ---- debug locally -------
    # host = "localhost"
    # user = "testuser"
    # password = ""
    # db_name = "boss"
    # port = 3306

    return pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db_name,
                           port=port,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def send_sns_alert(topic_arn, message, session=None, subject=None):
    """
    Send error message to given SNS topic.

    If sending fails, an error will be logged.

    Args:
        topic_arn (string): SNS topic to use for error message.
        message (string): Error message to send.
        session (optional[Session]) Boto session.

    Returns:
        (bool): False if SNS alert failed.
    """
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('sns')
    args = {
        "TopicArn": topic_arn,
        "Message":  message
    }
    if subject is not None:
        args["Subject"] = subject
    resp = client.publish(**args)
    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        LOG.error(
            "Unable to send following error to SNS Topic. Received the following HTTPStatusCode {}: {}".format(
                resp["ResponseMetadata"]["HTTPStatusCode"], message))
        return False

    return True


def query_for_deletes(data, session=None):
    """
    Queries for data to be deleted and kicks off delete step function

    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session

    Returns:
        (Dict): Data dictionary passed in.
    """
    LOG.debug("query_for_deletes() entering.")
    if session is None:
        session = bossutils.aws.get_session()
        LOG.debug("created session")
    sfn_client = session.client('stepfunctions')
    LOG.debug("created sfn_client")

    data = query_for_deletes_channels(data, session, sfn_client)
    LOG.debug("finished query_for_deletes_channels")
    if not data.pop(DATA_FOUND, None):
        LOG.debug("no data found after channels")
        data = query_for_deletes_experiments(data, session, sfn_client)
        LOG.debug("finished query_for_deletes_experiments")
        if not data.pop(DATA_FOUND, None):
            LOG.debug("no data found after experiments")
            data = query_for_deletes_collections(data, session, sfn_client)
            LOG.debug("finished query_for_deletes_collections")
            if not data.pop(DATA_FOUND, None):
                LOG.debug("no data found after collections")
                data = query_for_deletes_coord_frames(data, session, sfn_client)
                LOG.debug("finished query_for_deletes_coord_frames")

    LOG.debug("query_for_deletes() exiting.")

    return data

def query_for_deletes_coord_frames(data, session, sfn_client):
    """
    Finds coordinate frames marked for deletion and starts delete step function.

    Note that coordinate frames don't currently support metadata so the
    lookup table is not checked.

    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session
        sfn_client(StepFunction): AWS step function client

    Returns:
        (dict): Returns data dictionary that was passed in.
    """
    LOG.debug("query_for_deletes_coord_frames() entering.")
    connection = get_db_connection(data)
    LOG.debug("created pymysql connection")
    try:
        with connection.cursor() as cf_cursor:
            one_day_ago = datetime.now() - timedelta(days=1, hours=12)
            sql = ("SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `coordinate_frame` where "
                   "`to_be_deleted` < %s AND `deleted_status` is null OR "
                   "`deleted_status` = %s")
            cf_cursor.execute(sql, (one_day_ago, DELETED_STATUS_FINISHED))
            row_count = 0
            for cf_row in cf_cursor:
                row_count += 1
                LOG.info("found coord frame: " + str(cf_row))

                cf_id = cf_row['id']

                data["coordinate_frame_id"] = cf_id

                if cf_row['deleted_status'] != DELETED_STATUS_FINISHED:
                    sql = "UPDATE `coordinate_frame` SET `deleted_status`=%s WHERE `id`=%s"
                    with connection.cursor() as del_cursor:
                        del_cursor.execute(sql, (DELETED_STATUS_START, str(cf_id)))
                        connection.commit()

                LOG.debug("about to start coord frame delete step fcn")
                response = sfn_client.start_execution(
                    stateMachineArn=data["delete-coord-frame-sfn-arn"],
                    name="delete-boss-coord-frame-{}".format(uuid.uuid4().hex),
                    input=json.dumps(data)
                )
                LOG.debug(response)

            LOG.debug("found {} coord frames to delete".format(row_count))
    finally:
        connection.close()

    data[DATA_FOUND] = row_count > 0
    LOG.debug("query_for_deletes_coord_frames() exiting.")
    return data


def query_for_deletes_collections(data, session, sfn_client):
    """
    Finds collections marked for deletion and starts delete step function.

    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session
        sfn_client(StepFunction): AWS step function client

    Returns:
        (dict): Returns data dictionary that was passed in.
    """
    LOG.debug("query_for_deletes_collections() entering.")
    connection = get_db_connection(data)
    LOG.debug("created pymysql connection")
    try:
        with connection.cursor() as coll_cursor:
            one_day_ago = datetime.now() - timedelta(days=1, hours=12)
            sql = ("SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `collection` where "
                   "`to_be_deleted` < %s AND `deleted_status` is null OR " 
                   "`deleted_status` = %s")
            coll_cursor.execute(sql, (one_day_ago, DELETED_STATUS_FINISHED))
            row_count = 0
            for coll_row in coll_cursor:
                row_count += 1
                LOG.info("found collection: " + str(coll_row))

                coll_id = coll_row['id']
                with connection.cursor() as lookup_cursor:
                    # get lookup key given collection id and collection name
                    sql = "SELECT `id`, `collection_name`, `lookup_key` FROM `lookup` where `collection_name`=%s AND `experiment_name` IS null AND `channel_name` IS null"
                    lookup_cursor.execute(sql, (coll_row['name'],))
                    lookup_key_id = None
                    lookup_key = None
                    for lookup_row in lookup_cursor:
                        # its possible for two collections to have the same name so we search for the one with the
                        # correct collection id.
                        if int(lookup_row['lookup_key']) == coll_id:
                            lookup_key_id = lookup_row['id']
                            lookup_key = lookup_row['lookup_key']
                            break
                    if lookup_key_id is None and coll_row['deleted_status'] != DELETED_STATUS_FINISHED:
                        # If status is finished, first attempt at deleting row
                        # failed due to a foreign key constraint.

                        LOG.warning('coll_id {} did not have an associated lookup_key in the lookup'
                                    .format(coll_id))
                        send_sns_alert(
                            data['topic-arn'],
                            'Delete Error: channel id {}, has no lookup key in the endpoint lookup table.'.format(coll_id),
                            subject="AWS Notification: BOSS delete error {} no lookup key for coll id {}".format(data['db'], coll_id))

                        sql = "UPDATE collection SET deleted_status=%s WHERE `id`=%s"
                        lookup_cursor.execute(sql, (DELETED_STATUS_ERROR, str(coll_id)))
                        connection.commit()

                    else:
                        data["lookup_key"] = lookup_key
                        data["lookup_key_id"] = lookup_key_id
                        data["collection_id"] = coll_id

                        if coll_row['deleted_status'] != DELETED_STATUS_FINISHED:
                            sql = "UPDATE collection SET deleted_status=%s WHERE `id`=%s"
                            lookup_cursor.execute(sql, (DELETED_STATUS_START, str(coll_id)))
                            connection.commit()

                        LOG.debug("about to start collection delete step fcn")
                        response = sfn_client.start_execution(
                            stateMachineArn=data["delete-coll-sfn-arn"],
                            name="delete-boss-coll-{}".format(uuid.uuid4().hex),
                            input=json.dumps(data)
                        )
                        LOG.debug(response)

            LOG.debug("found {} collections to delete".format(row_count))
    finally:
        connection.close()

    data[DATA_FOUND] = row_count > 0
    LOG.debug("query_for_deletes_collections() exiting.")
    return data


def query_for_deletes_experiments(data, session, sfn_client):
    """
    Finds experiments marked for deletion and starts delete step function.

    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session
        sfn_client(StepFunction): AWS step function client

    Returns:
        (dict): Returns data dictionary that was passed in.
    """
    LOG.debug("query_for_deletes_experiments() entering.")
    connection = get_db_connection(data)
    LOG.debug("created pymysql connection")
    try:
        with connection.cursor() as exp_cursor:
            one_day_ago = datetime.now() - timedelta(days=1, hours=12)
            sql = ("SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `experiment` where "
                   "`to_be_deleted` < %s AND `deleted_status` is null OR "
                   "`deleted_status` = %s")
            exp_cursor.execute(sql, (one_day_ago, DELETED_STATUS_FINISHED))
            row_count = 0
            for exp_row in exp_cursor:
                row_count += 1
                LOG.info("found experiment: " + str(exp_row))

                exp_id = exp_row['id']
                with connection.cursor() as lookup_cursor:
                    # get lookup key given channel id and channel name
                    sql = "SELECT `id`, `experiment_name`, `lookup_key` FROM `lookup` where `experiment_name`=%s AND `channel_name` IS null"
                    lookup_cursor.execute(sql, (exp_row['name'],))
                    lookup_key_id = None
                    lookup_key = None
                    for lookup_row in lookup_cursor:
                        # its possible for two channels to have the same name so we search for the one with the
                        # correct channel id.
                        parts = lookup_row['lookup_key'].split("&")
                        if int(parts[1]) == exp_id:
                            lookup_key_id = lookup_row['id']
                            lookup_key = lookup_row['lookup_key']
                            break
                    if lookup_key_id is None and exp_row['deleted_status'] != DELETED_STATUS_FINISHED:
                        # If status is finished, first attempt at deleting row
                        # failed due to a foreign key constraint.

                        LOG.warning('exp_id {} did not have an associated lookup_key in the lookup'
                                    .format(exp_id))
                        send_sns_alert(
                            data['topic-arn'],
                            'Delete Error: experiment id {}, has no lookup key in the endpoint lookup table.'.format(exp_id),
                            subject="AWS Notification: BOSS delete error {} no lookup key for exp id {}".format(data['db'], exp_id))

                        sql = "UPDATE experiment SET deleted_status=%s WHERE `id`=%s"
                        lookup_cursor.execute(sql, (DELETED_STATUS_ERROR, str(exp_id)))
                        connection.commit()

                    else:
                        data["lookup_key"] = lookup_key
                        data["lookup_key_id"] = lookup_key_id
                        data["experiment_id"] = exp_id

                        if exp_row['deleted_status'] != DELETED_STATUS_FINISHED:
                            sql = "UPDATE experiment SET deleted_status=%s WHERE `id`=%s"
                            lookup_cursor.execute(sql, (DELETED_STATUS_START, str(exp_id)))
                            connection.commit()

                        LOG.debug("about to start experiment delete step fcn")
                        response = sfn_client.start_execution(
                            stateMachineArn=data["delete-exp-sfn-arn"],
                            name="delete-boss-exp-{}".format(uuid.uuid4().hex),
                            input=json.dumps(data)
                        )
                        LOG.debug(response)

            LOG.debug("found {} experiments to delete".format(row_count))
    finally:
        connection.close()

    data[DATA_FOUND] = row_count > 0
    LOG.debug("query_for_deletes_experiments() exiting.")
    return data


def query_for_deletes_channels(data, session, sfn_client):
    """
    Queries for data to be deleted and kicks off delete step function
    adds the following values to data (values are examples only):
            "lookup_key": "36&26&31",
            "channel_id": "1",
            "lookup_key_id": "1",

    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session
        sfn_client(StepFunction): AWS step function client

    Returns:
        (Dict): Data dictionary passed in.
    """
    LOG.debug("query_for_deletes_channels() entering.")
    connection = get_db_connection(data)
    LOG.debug("created pymysql connection")
    try:
        with connection.cursor() as ch_cursor:
            channel_type = 'annotation'

            while True:
                # this query will find 1 item to be deleted that does not have a delete_status (good for debugging)
                # sql = ("SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `channel` where `to_be_deleted` "
                #        "is not null AND `deleted_status` is null and `type` =%s limit 1")
                # ch_cursor.execute(sql, (channel_type,))

                # This query version will only find items older than a 1.5 days (gives us time to stop deletes).
                one_day_ago = datetime.now() - timedelta(days=1, hours=12)
                sql = ("SELECT `id`, `to_be_deleted`, `name`, `deleted_status` FROM `channel` where "
                       "`to_be_deleted` < %s AND `deleted_status` is null and `type` = %s")
                ch_cursor.execute(sql, (one_day_ago, channel_type,))
                row_count = 0
                for ch_row in ch_cursor:
                    row_count += 1
                    LOG.info("found channel: " + str(ch_row))
                    print("found channel: " + str(ch_row))

                    with connection.cursor() as cursor:
                        # get lookup key given channel id and channel name
                        channel_id = ch_row['id']
                        sql = "SELECT `id`, `channel_name`, `lookup_key` FROM `lookup` where `channel_name`=%s"
                        cursor.execute(sql, (ch_row['name'],))
                        lookup_key_id = None
                        lookup_key = None
                        for lookup_row in cursor:
                            # its possible for two channels to have the same name so we search for the one with the
                            # correct channel id.
                            parts = lookup_row['lookup_key'].split("&")
                            if int(parts[2]) == channel_id:
                                lookup_key_id = lookup_row['id']
                                lookup_key = lookup_row['lookup_key']
                                break
                        if lookup_key_id is None:
                            LOG.warning('channel_id {} did not have an associated lookup_key in the lookup'
                                        .format(channel_id))
                            send_sns_alert(
                                    data['topic-arn'],
                                    'Delete Error: channel id {}, has no lookup key in the endpoint lookup table.'.format(channel_id),
                                    subject="AWS Notification: BOSS delete error {} no lookup key for ch id {}".format(data['db'], channel_id))
                            sql = "UPDATE channel SET deleted_status=%s WHERE `id`=%s"
                            cursor.execute(sql, (DELETED_STATUS_ERROR, str(channel_id),))
                            connection.commit()
                        else:
                            sql = "UPDATE channel SET deleted_status=%s WHERE `id`=%s"
                            cursor.execute(sql, (DELETED_STATUS_START, str(channel_id),))
                            connection.commit()
                            # Kick off step-function
                            data["lookup_key"] = lookup_key
                            data["lookup_key_id"] = lookup_key_id
                            data["channel_id"] = channel_id

                            LOG.debug("about to call sfn_client.start_execution")
                            response = sfn_client.start_execution(
                                stateMachineArn=data["delete-sfn-arn"],
                                name="delete-boss-{}".format(uuid.uuid4().hex),
                                input=json.dumps(data)
                            )
                            LOG.debug(response)

                if row_count == 0 and channel_type == "annotation":
                    # no annotation channels were deleted, loop again and delete any image channels
                    channel_type = "image"
                    LOG.debug("No annotation channels found to delete, now looking for image channels.")
                else:
                    break  # while loop
            LOG.debug("found {} channels to delete".format(row_count))

    finally:
        connection.close()

    data[DATA_FOUND] = row_count > 0
    LOG.debug("query_for_deletes_channels() exiting.")
    return data


def delete_metadata(data, session=None):
    """
    Deletes all metadata from DynamoDB table
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session

    Returns:
        (Dict): Data dictionary passed in.
    """
    LOG.debug("delete_metadata() entering.")

    if data["lookup_key"] is None:
        LOG.debug("delete_metadata() exiting - lookup_key is None.")
        return data

    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('dynamodb')

    lookup_key = data["lookup_key"]
    meta_db = data["meta-db"]
    query_params = {'TableName': meta_db,
                    'KeyConditionExpression': 'lookup_key = :lookup_key_value',
                    'ExpressionAttributeValues': {":lookup_key_value": {"S": lookup_key}},
                    'ExpressionAttributeNames': {"#bosskey": "key"},
                    'ProjectionExpression': "lookup_key, #bosskey",
                    'ConsistentRead': True,
                    'Limit': 100}
    query_resp = client.query(**query_params)

    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(
            "Error querying bossmeta dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))

    LOG.debug("finished querying for metadata.")
    count = 0
    while query_resp['Count'] > 0:
        exclusive_start_key = None
        for meta in query_resp["Items"]:
            exclusive_start_key = meta
            count += 1
            del_params = {'TableName': meta_db,
                          'Key': meta,
                          'ReturnValues': 'NONE',
                          'ReturnConsumedCapacity': 'NONE'}
            del_resp = client.delete_item(**del_params)
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                LOG.info("response is {}, about to raise DeleteError".format(
                    del_resp["ResponseMetadata"]["HTTPStatusCode"]))
                raise DeleteError(
                    "Error deleting from bossmeta dynamoDB table, received HTTPStatusCode: {}, using params: {}, "
                    .format(del_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(del_params)))
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying bossmeta dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    LOG.debug("deleted {} metadata items".format(count))
    LOG.debug("delete_metadata() exiting.")
    return data


def get_channel_key(lookup_key):
    base_key = '{}'.format(lookup_key)
    hash_str = hashlib.md5(base_key.encode()).hexdigest()
    return '{}&{}'.format(hash_str, base_key)


def delete_id_count(data, session=None):
    """
    Deletes id count for lookup key.
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:
        (Dict): Data dictionary passed in.
    """
    LOG.debug("delete_id_count() entering.")
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('dynamodb')

    id_count_table = data["id-count-table"]
    lookup_key = data["lookup_key"]
    channel_key = get_channel_key(lookup_key)

    query_params = {'TableName': id_count_table,
                    'KeyConditionExpression': '#channel_key = :channel_key_value',
                    'ExpressionAttributeValues': {":channel_key_value": {"S": channel_key}},
                    'ExpressionAttributeNames': {"#channel_key": "channel-key", "#version": "version"},
                    'ProjectionExpression': "#channel_key, #version",
                    'ConsistentRead': True,
                    'Limit': 100}
    query_resp = client.query(**query_params)
    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(
            "Error querying idCount dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    LOG.debug("finished query id count table")

    count = 0
    exclusive_start_key = None
    while query_resp['Count'] > 0:
        for id_key in query_resp["Items"]:
            exclusive_start_key = id_key
            count += 1
            print("deleting: {}".format(id_key))
            del_params = {'TableName': id_count_table,
                          'Key': id_key,
                          'ReturnValues': 'NONE',
                          'ReturnConsumedCapacity': 'NONE'}
            del_resp = client.delete_item(**del_params)
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DeleteError(
                    "Error deleting from idCount dynamoDB table, received HTTPStatusCode: {}, using params: {}"
                    .format(del_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(del_params)))
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying idCount dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    LOG.debug("deleted {} id_count items".format(count))
    LOG.debug("delete_id_count() exiting.")
    return data


def get_channel_id_key(lookup_key, resolution, id_key):
    base_key = '{}&{}&{}'.format(lookup_key, resolution, id_key)
    hash_str = hashlib.md5(base_key.encode()).hexdigest()
    return '{}&{}'.format(hash_str, base_key)


def delete_id_index(data, session=None):
    """
    Deletes id index data for lookup key.
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-index-table
        session(Session): AWS boto3 Session

    Returns:
        (Dict): Data dictionary passed in.
    """
    LOG.debug("delete_id_index() entering.")
    if session is None:
        session = bossutils.aws.get_session()
    id_index_table = data["id-index-table"]
    lookup_key = data["lookup_key"]
    client = session.client('dynamodb')
    and_lookup_key = "&{}&".format(lookup_key)

    query_params = {'TableName': id_index_table,
                    # 'ScanFilter': { '"#channel_id_key":': {'AttributeValueList': [{"S": ":channel_id_key_value"}],
                    #                                        'ComparisonOperator': "CONTAINS"}},
                    'FilterExpression': 'contains(#channel_id_key, :channel_id_key_value)',
                    'ExpressionAttributeValues': {":channel_id_key_value": {"S": and_lookup_key}},
                    'ExpressionAttributeNames': {"#channel_id_key": "channel-id-key", "#version": "version"},
                    'ProjectionExpression': "#channel_id_key, #version",
                    'ConsistentRead': True,
                    'Limit': 100}
    scan_resp = client.scan(**query_params)
    if scan_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(
            "Error scanning idIndex dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                scan_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    LOG.debug("delete_id_index: finshed querying id index")

    count = 0
    exclusive_start_key = None
    while scan_resp['Count'] > 0:
        for id_key in scan_resp["Items"]:
            exclusive_start_key = id_key
            count += 1
            del_resp = client.delete_item(
                TableName=id_index_table,
                Key=id_key,
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE')
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                del_resp["deleting"] = id_key
                raise DeleteError(del_resp)
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        scan_resp = client.scan(**query_params)
        if scan_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying idIndex dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    scan_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    LOG.debug("deleted {} id_index items".format(count))
    LOG.debug("delete_id_index() exiting.")
    return data


def put_json_in_s3(client, bucket_name, key, py_object):
    """
    takes a python list or dict, converts it to json and pushes it as into s3.
    Args:
        client(boto3.Client): s3 client
        bucket_name: name of the bucket to store the object
        key: Object key
        py_object: Python list or dict to be stored

    Returns:

    """
    json_object = json.dumps(py_object)
    resp = client.put_object(
        Body=json_object.encode("utf-8"),
        Bucket=bucket_name,
        Key=key
    )
    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        error = "Unable to put json object in s3 bucket, {}, put_object received " \
                "HTTPStatusCode {}".format(bucket_name, resp["ResponseMetadata"]["HTTPStatusCode"])
        LOG.error(error)
        raise DeleteError(error)


def get_json_from_s3(client, bucket_name, key):
    """
    pulls the json object from s3 and converts to python
    Args:
        client(boto3.Client): s3 client
        bucket_name: name of the bucket pull the object from
        key: Object key

    Returns:
        (List or Dict): Python list or dict that was in the s3 object
    """
    resp = client.get_object(
        Bucket=bucket_name,
        Key=key
    )
    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        error = "Unable to put json object in s3 bucket, {}, put_object received " \
                "HTTPStatusCode {}".format(bucket_name, resp["ResponseMetadata"]["HTTPStatusCode"])
        LOG.error(error)
        raise DeleteError(error)
    json_object = json.loads(resp["Body"].read().decode('utf8'))
    return json_object


def get_exclusive_key(s3_index_row):
    """
    returns the exclusive key needed to make further queries of the s3Index dynamo table.
    Args:
        s3_index_row(dict): dynamo table row for the s3Index

    Returns:
        (dict): exclusive key
    """
    exclusive = {"ingest-job-hash": s3_index_row["ingest-job-hash"],
                 "ingest-job-range": s3_index_row["ingest-job-range"]}
    return exclusive


def get_primary_key(s3_index_row):
    """
    returns the primary key of the s3Index dynamo table.
    Args:
        s3_index_row(dict): dynamo table row for the s3Index

    Returns:
        (dict): primary key
    """
    primary = {"object-key": s3_index_row["object-key"],
               "version-node": s3_index_row["version-node"]}
    return primary


def merge_parallel_outputs(data):
    """
    parallel outputs returns an array of dicts, one from each parallel output. This method combines the dicts into
    a single dict.
    Args:
        data(list(dists): List of dicts

    Returns:
        (dict) merged dict
    """
    merge = {}
    for items in data:
        merge.update(items)
    return merge

class S3IndexExtractor:
    """
    Searches the s3 index table for all cuboids that are part of the channel
    to be deleted.  Uses the GSI named by the constant INGEST_ID_INDEX to find
    cuboids from the various ingest jobs.

    Attributes:
        data (dict): Step function input data.
        session (Session): Open Boto3 session.
        s3client (S3.Client): Boto3 S3 client.
        dynamo (DynamoDB.Client): Boto3 DynamoDB client.
        paginator (DynamoDB.Paginator.Query): Paginator for DynamoDB query results.
        s3_index_table (str): Name of DynamoDB s3 index table.

    """
    def __init__(self, data, session=None):
        """
        Constructor.

        Args:
            data (dict): Step function input data.
            session (optional[Session]): Boto3 session.
        """
        self.session = session
        if self.session is None:
            self.session = bossutils.aws.get_session()

        self.data = data
        self.s3client = self.session.client('s3')
        self.delete_bucket = self.data['delete_bucket']
        self.s3_index_table = self.data['s3-index-table']
        self.dynamo = self.session.client('dynamodb')
        self.paginator = self.dynamo.get_paginator('query')

    def start(self, shard_index, max_id_suffix, max_items_per_shard):
        """
        Main entry point.  Starts query of s3 index table and writes cuboid object
        keys to delete bucket.

        Args:
            shard_index (list[str]): Stores object keys of shards in the delete bucket.
            max_id_suffix (int): Max number (inclusive) that could be appended to key in the s3 index's GSI.
            max_items_per_shard (int): Max number of cuboid object keys to store in one shard.

        Returns:
            (dict): The original step function input with a new key: delete_shard_index_key.  This holds the object key of the shard list.
        """
        lookup_key = self.data["lookup_key"]
        col, exp, chan = lookup_key.split("&")

        # Check cuboids added via cutouts.
        cutout_cuboids = [ResJobId(res=0, job_id=CUTOUT_JOB_ID)]
        res_job_ids = self._get_resolutions_and_job_ids(col, exp, chan)

        # Combine lists of cutout and ingest job cuboids.
        res_job_ids = cutout_cuboids + res_job_ids

        shard_list = []
        item_count = 0
        for res_id in res_job_ids:
            for i in range(max_id_suffix+1):
                key = AWSObjectStore.get_ingest_id_hash(col, exp, chan, res_id.res, res_id.job_id, i) 
                resp_iter = self._query_ingest_id_index(key)
                for resp in resp_iter:
                    for item in resp["Items"]:
                        shard_list.append(get_primary_key(item))
                        item_count += 1
                        if item_count == max_items_per_shard:
                            shard_key = '{}-del'.format(uuid.uuid4().hex)
                            shard_index.append(shard_key)
                            put_json_in_s3(self.s3client, self.delete_bucket, shard_key, shard_list)
                            shard_list = []
                            item_count = 0

        if item_count != 0:
            shard_key = '{}-del'.format(uuid.uuid4().hex)
            shard_index.append(shard_key)
            put_json_in_s3(self.s3client, self.delete_bucket, shard_key, shard_list)


        # Write master index to s3.
        delete_shard_index_key = "{}-index-del".format(uuid.uuid4().hex)
        put_json_in_s3(self.s3client, self.delete_bucket, delete_shard_index_key, shard_index)

        self.data['delete_shard_index_key'] = delete_shard_index_key
        return self.data

    def _get_resolutions_and_job_ids(self, col, exp, chan):
        """
        Queries the ingest job table and gets all jobs associated with the given
        channel.

        Args:
            col (int): Collection id.
            exp (int): Experiment id.
            chan (int): Channel id.

        Returns:
            (list[ResJobId])
        """
        sql_conn = get_db_connection(self.data)
        exp_query = 'SELECT num_hierarchy_levels FROM experiment WHERE id = %s'
        job_query_args = dict(col=col, exp=exp, chan=chan)
        job_query = (
            'SELECT id, resolution FROM ingest_job ' + 
            'WHERE collection_id = %(col)s AND ' +
            'experiment_id = %(exp)s AND ' +
            'channel_id = %(chan)s'
            )

        num_hierarchy_levels = 1
        res_job_ids = []

        try:
            with sql_conn.cursor(pymysql.cursors.SSCursor) as cursor:
                num_rows = cursor.execute(exp_query, str(exp))
                if num_rows > 0:
                    exp_row = cursor.fetchone()
                    num_hierarchy_levels = exp_row[0]

                num_rows = cursor.execute(job_query, job_query_args)
                if num_rows == 0:
                    return res_job_ids
                for row in cursor.fetchall_unbuffered():
                    res_job_ids.append(ResJobId(job_id=row[0], res=row[1]))
                    for i in range(row[1]+1, num_hierarchy_levels):
                        res_job_ids.append(ResJobId(job_id=row[0], res=i))
        finally:
            sql_conn.close()

        return res_job_ids

    def _query_ingest_id_index(self, key):
        """
        Query ingest-id-index of the s3 index table to retrieve all s3 object keys
        associated with a particular ingest.

        Args:
            key (str): Value of ingest-id-hash to search for.

        Returns:
            (list[response])
        """
        return self.paginator.paginate(
            TableName=self.s3_index_table, 
            IndexName=INGEST_ID_INDEX,
            KeyConditionExpression="#ingest_id_hash = :ingest_id_val",
            ExpressionAttributeNames={ "#ingest_id_hash": "ingest-id-hash" },
            ExpressionAttributeValues={ ":ingest_id_val": { "S": key } }
        )


def find_s3_index(data, session=None):
    """
    Search the s3 index for all cuboids stored in the s3 bucket.  Writes 
    batches of MAX_ITEMS_PER_SHARD s3 object keys to "shards" in the delete
    bucket so that they can be later deleted by another step function.

    Args:
        data (dict): Data passed into the step function.
        session (optional[Session]):

    Returns:
        (dict): The original step function input with a new key: delete_shard_index_key.  This holds the object key of the shard list.
    """
    s3_ind_ex = S3IndexExtractor(data, session)

    # Use the original deprecated GSI to find any old ingests related to the
    # deleted channel.
    shard_index = find_s3_index_using_legacy_index(data, s3_ind_ex.session)

    # Use new GSI to find ingests related to the deleted channel.
    return s3_ind_ex.start(shard_index, INGEST_ID_MAX_N, MAX_ITEMS_PER_SHARD)

def find_s3_index_using_legacy_index(data, session):
    """
    Find s3 index keys containing the lookup key write them to s3 to be deleted.  Split the list into
    s3 objects with KEYS_PER_S3_OBJECT each.  Also write an index Object contining the list of other objects.
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:
        (list[str]): list of shards written to s3
    """
    LOG.debug("find_s3_index_using_legacy_index() entering.")
    client = session.client('dynamodb')
    s3client = session.client('s3')

    s3_index_table = data["s3-index-table"]
    lookup_key = data["lookup_key"]
    delete_bucket = data["delete_bucket"]

    col, exp, ch = lookup_key.split("&")
    query_params = {'TableName': s3_index_table,
                    'IndexName': S3_INDEX_TABLE_INDEX,
                    'KeyConditionExpression': '#ingest_job_hash = :ingest_job_hash_value AND '
                                              'begins_with(#ingest_job_range, :ingest_job_range_value)',
                    'ExpressionAttributeValues': {":ingest_job_hash_value": {"S": col},
                                                  ":ingest_job_range_value": {"S": exp+"&"+ch}},
                    'ExpressionAttributeNames': {"#object_key": "object-key",
                                                 "#version_node": "version-node",
                                                 "#ingest_job_hash": "ingest-job-hash",
                                                 "#ingest_job_range": "ingest-job-range"},
                    'ProjectionExpression': "#object_key, #version_node, #ingest_job_hash, #ingest_job_range",
                    'Limit': 200}
    query_resp = client.query(**query_params)
    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(
            "Error querying s3index dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    shard_index = []
    shard_list = []
    shard_count = 0
    count = 0
    exclusive_start_key = None
    while query_resp['Count'] > 0:
        for id_key in query_resp["Items"]:
            exclusive_start_key = id_key
            count += 1
            shard_list.append(get_primary_key(id_key))
            shard_count += 1
            if shard_count == MAX_ITEMS_PER_SHARD:
                shard_key = "{}-del".format(uuid.uuid4().hex)
                shard_index.append(shard_key)
                put_json_in_s3(s3client, delete_bucket, shard_key, shard_list)
                shard_list = []
                shard_count = 0

        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError("Error querying s3index dynamoDB table, received HTTPStatusCode: {}, using params: {}"
                              .format(query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))

    if shard_list:
        shard_key = "{}-del".format(uuid.uuid4().hex)
        shard_index.append(shard_key)
        put_json_in_s3(s3client, delete_bucket, shard_key, shard_list)
    LOG.debug("found {} s3_index items".format(count))
    LOG.debug("find_s3_index_using_legacy_index() exiting.")
    return shard_index


def get_key_list(shard_list):
    """
    Takes a list of rows from the DynamoDB s3index table and converts them into a dictionary representing
    Key to be deleted from boto3.client.delete_objects()
    Args:
        shard_list(list): list of results from dynamoDB query of s3index table.

    Returns:
        (dict): of keys to be deleted in the format used by boto3.client.delete_objects()
    """
    delete_objects = []
    for row in shard_list:
        delete_objects.append({'Key': "{}&{}".format(row['object-key']['S'], row['version-node']['N'])})
    return {'Objects': delete_objects}


def delete_s3_index(data, session=None):
    """
    Deletes s3 index keys containing the lookup key after deleting the S3 Object from cuboid bucket
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in
    """
    LOG.debug("delete_s3_index() entering.")
    if session is None:
        session = bossutils.aws.get_session()
    dynclient = session.client('dynamodb')
    s3client = session.client('s3')
    s3_cuboid_bucket = data["cuboid_bucket"]
    s3_delete_bucket = data["delete_bucket"]
    s3_index_table = data["s3-index-table"]
    delete_shard_index_key = data["delete_shard_index_key"]

    shard_index = get_json_from_s3(s3client, s3_delete_bucket, delete_shard_index_key)
    count = 0
    for shard_list_name in shard_index:
        shard_list = get_json_from_s3(s3client, s3_delete_bucket, shard_list_name)
        # TODO SH Can implement delete faster using this method.  It will be sightly more complex to review the errors
        #  to avoid deleting DynamoDB entries when corresponding S3 objects failed to delete
        # key_list = get_key_list(shard_list)
        # response = cuboid_bucket.delete_objects(Bucket=s3_cuboid_bucket, Delete=key_list)

        for row in shard_list:
            s3_key = "{}&{}".format(row['object-key']['S'], row['version-node']['N'])
            s3_response = s3client.delete_object(Bucket=s3_cuboid_bucket, Key=s3_key)
            if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}".format(
                    s3_key, s3_cuboid_bucket, s3_response["HTTPStatusCode"]))

            delete_params = {'TableName': s3_index_table,
                             'Key': {'object-key': {'S': row["object-key"]["S"]},
                                     'version-node': {"N": row["version-node"]["N"]}}}
            count += 1
            query_resp = dynclient.delete_item(**delete_params)
            if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DeleteError(query_resp)
    print("deleted {} s3 objects and s3_index items".format(count))
    LOG.debug("delete_s3_index() exiting.")
    return data


def delete_clean_up(data, session=None):
    """
    first sets delete status to finished, then deletes the shard_index and the shard_lists from the delete_bucket,
    finally it deletes the channel and lookup entries.
    Args:
        data(Dict): Dictionary containing at least the following keys:  delete_bucket, delete_shard_index_key,
                    channel_id, db, lookup_key_id
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in.
    """
    LOG.debug("delete_clean_up() entering.")
    if session is None:
        session = bossutils.aws.get_session()
    s3client = session.client('s3')

    connection = get_db_connection(data)

    try:
        with connection.cursor() as cursor:
            LOG.debug("Updating deleted_status to finished.")
            sql = "UPDATE channel SET deleted_status=%s WHERE `id`=%s"
            cursor.execute(sql, (DELETED_STATUS_FINISHED, str(data["channel_id"]),))
            connection.commit()

            delete_bucket = data["delete_bucket"]
            delete_shard_index_key = data["delete_shard_index_key"]

            LOG.debug("Deleting shards in s3 table {}.".format(delete_bucket))
            shard_index = get_json_from_s3(s3client, delete_bucket, delete_shard_index_key)
            count = 0
            for shard_list_name in shard_index:
                s3_response = s3client.delete_object(Bucket=delete_bucket, Key=shard_list_name)
                if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                    raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}"
                                      .format(shard_list_name, delete_bucket, s3_response["HTTPStatusCode"]))
                count += 1

            s3_response = s3client.delete_object(Bucket=delete_bucket, Key=delete_shard_index_key)
            if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}".format(
                    delete_shard_index_key, delete_bucket, s3_response["HTTPStatusCode"]))
            print("deleted {} delete shard lists".format(count))

            # delete bosscore_source given channel_id as derived_channel_id or source_channel_id
            LOG.debug("Deleting from bosscore_source table")
            sql = "DELETE FROM `bosscore_source` where `derived_channel_id`=%s or `source_channel_id`=%s"
            cursor.execute(sql, (str(data["channel_id"]), str(data["channel_id"]),))
            connection.commit()

            # delete channel_related given channel_id as fromm_channel_id or to_channel_id
            LOG.debug("Deleting from channel_related table")
            sql = "DELETE FROM `channel_related` where `from_channel_id`=%s or `to_channel_id`=%s"
            cursor.execute(sql, (str(data["channel_id"]), str(data["channel_id"]),))
            connection.commit()

            # delete lookup_key given lookup_id
            LOG.debug("Deleting lookup_key from lookup table")
            sql = "DELETE FROM `lookup` where `id`=%s"
            cursor.execute(sql, (str(data["lookup_key_id"]),))
            connection.commit()

            # delete channel given channel id
            LOG.debug("Deleting channel from channel table")
            sql = "DELETE FROM `channel` where `id`=%s"
            cursor.execute(sql, (str(data["channel_id"]),))
            connection.commit()

    except Exception as ex:
        LOG.error("Error deleting channel: {}".format(ex))
        raise
    finally:
        connection.close()
    LOG.debug("delete_clean_up() exiting.")
    return data


def delete_experiment(data, session=None):
    """
    deletes experiement out of RDS lookup table and RDS experiment table.
    Args:
        data(Dict): Dictionary containing at least the following keys:  experiment_id, db, lookup_key_id
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in.
    """
    LOG.debug("delete_experiment() entering.")
    if session is None:
        session = bossutils.aws.get_session()

    connection = get_db_connection(data)

    try:
        with connection.cursor() as cursor:
            LOG.debug("Updating deleted_status to finished.")
            sql = "UPDATE experiment SET deleted_status=%s WHERE `id`=%s"
            cursor.execute(sql, (DELETED_STATUS_FINISHED, str(data["experiment_id"]),))
            connection.commit()

            # delete lookup_key given lookup_id, might be None if deleting
            # from the experiment table failed due to a foreign key
            # constraint
            if data["lookup_key_id"] is not None:
                LOG.debug("Deleting lookup_key from lookup table")
                sql = "DELETE FROM `lookup` where `id`=%s"
                cursor.execute(sql, (str(data["lookup_key_id"]),))
                connection.commit()

            # delete experiment given experiment id
            LOG.debug("Deleting experiment from experiment table")
            sql = "DELETE FROM `experiment` where `id`=%s"
            cursor.execute(sql, (str(data["experiment_id"]),))
            connection.commit()

    except Exception as ex:
        LOG.error("Error deleting experiment: {}".format(ex))
        raise
    finally:
        connection.close()
    LOG.debug("delete_experiment() exiting.")
    return data


def delete_coordinate_frame(data, session=None):
    """
    deletes coordinate_frame out of RDS lookup table and RDS coordinate_frame table.
    Args:
        data(Dict): Dictionary containing at least the following keys:  coordinate_frame_id, db, lookup_key_id
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in.
    """
    LOG.debug("delete_coordinate_frame() entering.")
    if session is None:
        session = bossutils.aws.get_session()

    connection = get_db_connection(data)

    try:
        with connection.cursor() as cursor:
            LOG.debug("Updating deleted_status to finished.")
            sql = "UPDATE coordinate_frame SET deleted_status=%s WHERE `id`=%s"
            cursor.execute(sql, (DELETED_STATUS_FINISHED, str(data["coordinate_frame_id"]),))
            connection.commit()

            # Currently no metadata for coord frames, so no lookup key.
            # delete lookup_key given lookup_id
            #LOG.debug("Deleting lookup_key from lookup table")
            #sql = "DELETE FROM `lookup` where `id`=%s"
            #cursor.execute(sql, (str(data["lookup_key_id"]),))
            #connection.commit()

            # delete coordinate_frame given coordinate_frame id
            LOG.debug("Deleting coordinate_frame from coordinate_frame table")
            sql = "DELETE FROM `coordinate_frame` where `id`=%s"
            cursor.execute(sql, (str(data["coordinate_frame_id"]),))
            connection.commit()
    except Exception as ex:
        LOG.error("Error deleting coord frame: {}".format(ex))
        raise
    finally:
        connection.close()
    LOG.debug("delete_coordinate_frame() exiting.")
    return data


def delete_collection(data, session=None):
    """
    deletes experiement out of RDS lookup table and RDS collection table.
    Args:
        data(Dict): Dictionary containing at least the following keys:  collection_id, db, lookup_key_id
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in.
    """
    LOG.debug("delete_collection() entering.")
    if session is None:
        session = bossutils.aws.get_session()

    connection = get_db_connection(data)

    try:
        with connection.cursor() as cursor:
            LOG.debug("Updating deleted_status to finished.")
            sql = "UPDATE collection SET deleted_status=%s WHERE `id`=%s"
            cursor.execute(sql, (DELETED_STATUS_FINISHED, str(data["collection_id"]),))
            connection.commit()

            # delete lookup_key given lookup_id, might be None if deleting
            # from the experiment table failed due to a foreign key
            # constraint
            if data["lookup_key_id"] is not None:
                LOG.debug("Deleting lookup_key from lookup table")
                sql = "DELETE FROM `lookup` where `id`=%s"
                cursor.execute(sql, (str(data["lookup_key_id"]),))
                connection.commit()

            # delete collection given collection id
            LOG.debug("Deleting collection from collection table")
            sql = "DELETE FROM `collection` where `id`=%s"
            cursor.execute(sql, (str(data["collection_id"]),))
            connection.commit()

    except Exception as ex:
        LOG.error("Error deleting collection: {}".format(ex))
        raise
    finally:
        connection.close()
    LOG.debug("delete_collection() exiting.")
    return data


def notify_admins(data, session=None):
    LOG.debug("notify_admins() entering.")
    if session is None:
        session = bossutils.aws.get_session()
    if not send_sns_alert(data["topic-arn"], data["error"], session):
        LOG.debug("notify_admins() raising DeleteError")
        raise DeleteError("Error notifying admins after delete failed.")

    LOG.debug("notify_admins() exiting.")
    return data


def delete_test_1(data, context=None):
    print("entered fcn delete_test_1")
    data["dt1"] = True
    pprint.pprint(data)
    return data


def delete_test_2(data, context=None):
    print("entered fcn delete_test_2")
    data["dt2"] = True
    pprint.pprint(data)
    return data


def delete_test_3(data, context=None):
    print("entered fcn delete_test_3")
    pprint.pprint(data)
    output = {
        'data': [1, 2, 3, 4],
        'index': 3  # zero indexed
    }
    pprint.pprint(output)
    return output


def delete_test_4(data):
    print("entered fcn delete_test_4")
    idx = data['index']
    data = data['data']
    print("Processing item: {}".format(data[idx]))
    data['index'] -= 1
    return data


def main():
    """
    Used for debugging other methods locally running from the command line.
    Returns:

    """
    input_from_main = {
        # "lookup_key": "1&1&1",
        # "channel_id": "1",
        # "lookup_key_id": "1",
        # "db": "localhost",
        "db": "endpoint-db.integration.boss",
        "meta-db": "bossmeta.integration.boss",
        "s3-index-table": "s3index.integration.boss",
        "id-index-table": "idIndex.integration.boss",
        "id-count-table": "idCount.integration.boss",
        "cuboid_bucket": "cuboids.integration.boss",
        "delete_bucket": "delete.integration.boss",
        "find-deletes-sfn-arn": "arn:aws:states:us-east-1:451493790433:stateMachine:FindDeletesIntegrationBoss",
        "delete-sfn-arn": "arn:aws:states:us-east-1:451493790433:stateMachine:DeleteCuboidIntegrationBoss",
        "delete-exp-sfn-arn": "arn:aws:states:us-east-1:451493790433:stateMachine:DeleteExperimentIntegrationBoss",
        "delete-coord-frame-sfn-arn": "arn:aws:states:us-east-1:451493790433:stateMachine:DeleteCoordframeIntegrationBoss",
        "delete-coll-sfn-arn": "arn:aws:states:us-east-1:451493790433:stateMachine:DeleteCollectionIntegrationBoss",
        "topic-arn": "arn:aws:sns:us-east-1:451493790433:ProductionMicronsMailingList",
        "error":  "test error for SFN",
    }
    session = boto3.session.Session(region_name="us-east-1")

    dict = query_for_deletes(input_from_main, session=session)
    # dict = delete_metadata(input_from_main, session=session)
    # dict = delete_id_count(dict, session=session)
    # dict = delete_id_index(dict, session=session)
    # dict = find_s3_index(dict, session=session)
    # dict = delete_s3_index(dict, session=session)
    # dict = delete_clean_up(dict, session=session)
    # #dict = notify_admins(dict, session=session)

    print("done.")


if __name__ == "__main__":
    main()
