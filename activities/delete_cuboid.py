#!/usr/bin/env python
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
"""

import boto3
import bossutils
import pprint
import hashlib
import uuid
import json


S3_INDEX_TABLE_INDEX = 'ingest-job-index'
MAX_ITEMS_PER_SHARD = 100

"""
DeleteError will be used if an Exception occurs within any of the delete functions.
"""
class DeleteError(Exception):
    pass


def delete_metadata(data, session=None):
    """
    Deletes all metadata from DynamoDB table
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session

    Returns:
        (Dict): Data dictionary passed in.
    """
    #if "meta-db" not in input:
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('dynamodb')

    lookup_key = data["lookup_key"]
    meta_db = data["meta-db"]
    query_params = {'TableName': meta_db,
                    'KeyConditionExpression':'lookup_key = :lookup_key_value',
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
    count = 0
    while query_resp['Count'] > 0:
        for meta in query_resp["Items"]:
            exclusive_start_key=meta
            count += 1
            del_params = {'TableName': meta_db,
                          'Key': meta,
                          'ReturnValues': 'NONE',
                          'ReturnConsumedCapacity': 'NONE'}
            del_resp = client.delete_item(**del_params)
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DeleteError(
                    "Error deleting from bossmeta dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                        del_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(del_params)))
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying bossmeta dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    print("deleted {} metadata items".format(count))
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

    count = 0
    while query_resp['Count'] > 0:
        for id in query_resp["Items"]:
            exclusive_start_key=id
            count += 1
            print("deleting: {}".format(id))
            del_params = {'TableName': id_count_table,
                          'Key': id,
                          'ReturnValues': 'NONE',
                          'ReturnConsumedCapacity': 'NONE'}
            del_resp = client.delete_item(**del_params)
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DeleteError(
                    "Error deleting from idCount dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                        del_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(del_params)))
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying idCount dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    print("deleted {} id_count items".format(count))
    return data


def get_channel_id_key(lookup_key, resolution, id):
    base_key = '{}&{}&{}'.format(lookup_key, resolution, id)
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
    if session is None:
        session = bossutils.aws.get_session()
    id_index_table = data["id-index-table"]
    lookup_key = data["lookup_key"]
    client = session.client('dynamodb')
    and_lookup_key = "&{}&".format(lookup_key)
    #and_lookup_key = "45aeef7ae7626d34f32c14512b25b1fa&19&14&16&0&1478"

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

    count = 0
    while scan_resp['Count'] > 0:
        for id in scan_resp["Items"]:
            exclusive_start_key=id
            count += 1
            del_resp = client.delete_item(
                TableName=id_index_table,
                Key=id,
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE')
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                del_resp["deleting"] = id
                raise DeleteError(del_resp)
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        scan_resp = client.scan(**query_params)
        if scan_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(
                "Error querying idIndex dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                    scan_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))
    print("deleted {} id_index items".format(count))
    return data


def put_json_in_s3(client, bucket_name, key, object):
    """
    takes a python list or dict, converts it to json and pushes it as into s3.
    Args:
        client(boto3.Client): s3 client
        bucket_name: name of the bucket to store the object
        key: Object key
        object: Python list or dict to be stored

    Returns:

    """
    json_object = json.dumps(object)
    response = client.put_object(
        Body=json_object.encode("utf-8"),
        Bucket=bucket_name,
        Key=key
    )

def get_json_from_s3(client, bucket_name, key):
    """
    pulls the json object from s3 and converts to python
    Args:
        bucket_name: name of the bucket pull the object from
        key: Object key

    Returns:
        (List or Dict): Python list or dict that was in the s3 object
    """
    response = client.get_object(
        Bucket=bucket_name,
        Key=key
    )
    json_object = json.loads(response["Body"].read().decode('utf8'))
    return json_object



def get_exclusive_key(s3_index_row):
    """
    returns the exclusive key needed to make further queries of the s3Index dynamo table.
    Args:
        s3_index_row(dict): dynamo table row for the s3Index

    Returns:
        (dict): exclusive key
    """
    exclusive = {}
    exclusive["ingest-job-hash"] = s3_index_row["ingest-job-hash"]
    exclusive["ingest-job-range"] = s3_index_row["ingest-job-range"]
    return exclusive


def get_primary_key(s3_index_row):
    """
    returns the primary key of the s3Index dynamo table.
    Args:
        s3_index_row(dict): dynamo table row for the s3Index

    Returns:
        (dict): primary key
    """
    primary = {}
    primary["object-key"] = s3_index_row["object-key"]
    primary["version-node"] = s3_index_row["version-node"]
    return primary

def find_s3_index(data, session=None):
    """backoff rate
    Find s3 index keys containing the lookup key write them to s3 to be deleted.  Split the list into
    s3 objects with KEYS_PER_S3_OBJECT each.  Also write an index Object contining the list of other objects.
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in
    """
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('dynamodb')
    s3client = session.client('s3')

    s3_index_table = data["s3-index-table"]
    lookup_key = data["lookup_key"]
    delete_bucket = data["delete_bucket"]
    delete_shard_index_key = "{}-index-del".format(uuid.uuid4().hex)

    col, exp, ch = lookup_key.split("&")
    query_params = {'TableName': s3_index_table,
                    'IndexName': S3_INDEX_TABLE_INDEX,
                    'KeyConditionExpression': '#ingest_job_hash = :ingest_job_hash_value AND begins_with(#ingest_job_range, :ingest_job_range_value)',
                    'ExpressionAttributeValues': {":ingest_job_hash_value": {"S": col}, ":ingest_job_range_value": {"S": exp+"&"+ch}},
                    'ExpressionAttributeNames': {"#object_key": "object-key", "#version_node": "version-node",
                                                 "#ingest_job_hash": "ingest-job-hash", "#ingest_job_range": "ingest-job-range"},
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
    while query_resp['Count'] > 0:
        for id in query_resp["Items"]:
            exclusive_start_key=id
            count += 1
            shard_list.append(get_primary_key(id))
            shard_count += 1
            if shard_count == MAX_ITEMS_PER_SHARD:
                shard_key = "{}-del".format(uuid.uuid4().hex)
                shard_index.append(shard_key)
                put_json_in_s3(s3client, delete_bucket, shard_key, shard_list)
                shard_list = []

        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError("Error querying s3index dynamoDB table, received HTTPStatusCode: {}, using params: {}, ".format(
                query_resp["ResponseMetadata"]["HTTPStatusCode"], json.dumps(query_params)))

    if shard_list:
        shard_key = "{}-del".format(uuid.uuid4().hex)
        shard_index.append(shard_key)
        put_json_in_s3(s3client, delete_bucket, shard_key, shard_list)
    put_json_in_s3(s3client, delete_bucket, delete_shard_index_key, shard_index)
    data["delete_shard_index_key"] = delete_shard_index_key
    print("found {} s3_index items".format(count))
    return data


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
    return { 'Objects': delete_objects}


def delete_s3_index(data, session=None):
    """
    Deletes s3 index keys containing the lookup key after deleting the S3 Object from cuboid bucket
    Args:
        data(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in
    """
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('dynamodb')
    s3client = session.client('s3')
    s3_cuboid_bucket = data["cuboid_bucket"]
    s3_delete_bucket = data["delete_bucket"]
    s3_index_table = data["s3-index-table"]
    lookup_key = data["lookup_key"]
    delete_shard_index_key = data["delete_shard_index_key"]

    shard_index = get_json_from_s3(s3client, s3_delete_bucket, delete_shard_index_key)
    count = 0
    for shard_list_name in shard_index:
        shard_list =  get_json_from_s3(s3client, s3_delete_bucket, shard_list_name)
        # TODO SH Can implement delete faster using this method.  It will be sightly more complex to review the errors and avoid deleting errored objects from DynamoDB
        #key_list = get_key_list(shard_list)
        #response = cuboid_bucket.delete_objects(Bucket=s3_cuboid_bucket, Delete=key_list)

        for row in shard_list:
            s3_key = "{}&{}".format(row['object-key']['S'], row['version-node']['N'])
            s3_response = s3client.delete_object(Bucket=s3_cuboid_bucket, Key=s3_key)
            if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}".format(
                    s3_key, s3_cuboid_bucket, s3_response["HTTPStatusCode"]))

            delete_params = {'TableName': s3_index_table,
                            'Key': {'object-key': {'S': row["object-key"]["S"]}, 'version-node': {"N": row["version-node"]["N"]}}}
            count += 1
            query_resp = client.delete_item(**delete_params)
            if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DeleteError(query_resp)
    print("deleted {} s3_index items".format(count))
    return data


def delete_clean_up(data, session=None):
    """
    deletes the shard_index and the shard_lists from the delete_bucket
    Args:
        data(Dict): Dictionary containing at least the following keys:  delete_bucket, delete_shard_index_key
        session(Session): AWS boto3 Session

    Returns:
        (Dict): data dictionary passed in.
    """
    if session is None:
        session = bossutils.aws.get_session()
    s3client = session.client('s3')
    delete_bucket = data["delete_bucket"]
    delete_shard_index_key = data["delete_shard_index_key"]

    shard_index = get_json_from_s3(s3client, delete_bucket, delete_shard_index_key)
    count = 0
    for shard_list_name in shard_index:
        s3_response = s3client.delete_object(Bucket=delete_bucket, Key=shard_list_name)
        if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}".format(
                shard_list_name, delete_bucket, s3_response["HTTPStatusCode"]))
        count += 1

    s3_response = s3client.delete_object(Bucket=delete_bucket, Key=delete_shard_index_key)
    if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        raise DeleteError("Error deleting s3 object, {}, from bucket, {}, received HTTPStatusCode: {}".format(
            delete_shard_index_key, delete_bucket, s3_response["HTTPStatusCode"]))
    print("deleted {} delete shard lists".format(count))
    return data

def save_and_delete(data, session=None):
    # if session is None:
    #     session = bossutils.aws.get_session()
    print("Need to perform save error here: {}".format(data["error-info"]))
    return data

def notify_admins(data, session=None):
    if session is None:
        session = bossutils.aws.get_session()
    client = session.client('sns')
    resp = client.publish(TopicArn=data["notify_topic"], Message=data["error"])
    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError("Error notifying admins after delete failed.")
    return data


def delete_test_1(input, context=None):
    print("entered fcn delete_test_1")
    input["dt1"] = True
    pprint.pprint(input)
    return input


def delete_test_2(input, context=None):
    print("entered fcn delete_test_2")
    input["dt2"] = True
    pprint.pprint(input)
    return input


def delete_test_3(input, context=None):
    print("entered fcn delete_test_3")
    pprint.pprint(input)
    outpule, names.pyt = {
        'data': [1,2,3,4],
        'index': 3 # zero indexed
    }
    pprint.pprint(output)
    return output


def delete_test_4(input_):
    print("entered fcn delete_test_4")
    idx = input_['index']
    data = input_['data']
    print("Processing item: {}".format(data[idx]))
    input_['index'] -= 1
    return input_


if __name__ == "__main__":
    input_from_main = {
        "lookup_key": "18&13&15",
        "db": "endpoint-db.hiderrt1.boss",
        "meta-db": "bossmeta.hiderrt1.boss",
        "s3-index-table": "s3index.hiderrt1.boss",
        "id-index-table": "idIndex.hiderrt1.boss",
        "id-count-table": "idCount.hiderrt1.boss",
        "cuboid_bucket": "cuboids.hiderrt1.boss",
        "delete_bucket": "delete.hiderrt1.boss",
        "delete-sfn-arn": "arn:aws:states:us-east-1:256215146792:stateMachine:Delete-cuboidHiderrt1Boss",
        "topic-arn": "arn:aws:sns:us-east-1:256215146792:ProductionMicronsMailingList",
        "error":  "test error for SFN"
    }
    session = boto3.session.Session(region_name="us-east-1")
    s3client = session.client("s3")

    #dict = delete_metadata(input_from_main, session=session)
    #dict = delete_id_count(dict, session=session)
    #dict = delete_id_index(dict, session=session)
    #dict = find_s3_index(dict, session=session)
    #dict = delete_s3_index(dict, session=session)
    #dict = delete_clean_up(dict, session=session)
    #dict = notify_admins(input_from_main, session=session)
    print("done.")
