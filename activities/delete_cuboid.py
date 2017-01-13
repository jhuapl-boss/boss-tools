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
This file holds the functions need to perform deletes for Collections, Experiment, Channel and Coordinate Frame
These may be run in Lambdas or Activities in Setup Functions.
"""

import boto3
import bossutils
import pprint
from boto3.dynamodb.conditions import Key, Attr
import hashlib

TEST_TABLE = 'hiderrt1-test1'
S3_INDEX_TABLE_INDEX = 'ingest-job-index'

"""
DeleteError will be used if an Exception occurs within any of the delete functions.
"""
class DeleteError(Exception):
    pass


def delete_metedata(input, session=None):
    """
    Deletes all metadata from DynamoDB table
    Args:
        input(Dict): Dictionary containing following keys: lookup_key, meta-db
        session(Session): AWS boto3 Session

    Returns:

    """
    #if "meta-db" not in input:
    lookup_key = input["lookup_key"]
    meta_db = input["meta-db"]
    session = bossutils.aws.get_session()
    client = session.client('dynamodb')
    query_params = {'TableName': meta_db,
                    'KeyConditionExpression':'lookup_key = :lookup_key_value',
                    'ExpressionAttributeValues': {":lookup_key_value": {"S": lookup_key}},
                    'ExpressionAttributeNames': {"#bosskey": "key"},
                    'ProjectionExpression': "lookup_key, #bosskey",
                    'ConsistentRead': True,
                    'Limit': 100}
    query_resp = client.query(**query_params)
    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(query_resp)
    count = 0
    while query_resp['Count'] > 0:
        for meta in query_resp["Items"]:
            exclusive_start_key=meta
            count += 1
            print("deleting: {}".format(meta))
            del_resp = client.delete_item(
                TableName='bossmeta.hiderrt1.boss',
                Key=meta,
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE')
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                pprint.pprint(del_resp)
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise DeleteError(query_resp)
    print("deleted {} items".format(count))


def get_channel_key(lookup_key):
    base_key = '{}'.format(lookup_key)
    hash_str = hashlib.md5(base_key.encode()).hexdigest()
    return '{}&{}'.format(hash_str, base_key)


def delete_id_count(input, session=None):
    """
    Deletes id count for lookup key.
    Args:
        input(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:

    """
    id_count_table = input["id-count-table"]
    lookup_key = input["lookup_key"]
    channel_key = get_channel_key(lookup_key)

    session = bossutils.aws.get_session()
    client = session.client('dynamodb')
    query_params = {'TableName': id_count_table,
                    'KeyConditionExpression': '#channel_key = :channel_key_value',
                    'ExpressionAttributeValues': {":channel_key_value": {"S": channel_key}},
                    'ExpressionAttributeNames': {"#channel_key": "channel-key", "#version": "version"},
                    'ProjectionExpression': "#channel_key, #version",
                    'ConsistentRead': True,
                    'Limit': 100}
    query_resp = client.query(**query_params)
    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(query_resp)

    count = 0
    while query_resp['Count'] > 0:
        for id in query_resp["Items"]:
            exclusive_start_key=id
            count += 1
            print("deleting: {}".format(id))
            del_resp = client.delete_item(
                TableName=id_count_table,
                Key=id,
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE')
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                del_resp["deleting"] = id
                raise DeleteError(del_resp)
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            del_resp["deleting"] = id
            raise DeleteError(query_resp)
    print("deleted {} items".format(count))


def get_channel_id_key(lookup_key, resolution, id):
    base_key = '{}&{}&{}'.format(lookup_key, resolution, id)
    hash_str = hashlib.md5(base_key.encode()).hexdigest()
    return '{}&{}'.format(hash_str, base_key)


def delete_id_index(input, session=None):
    """
    Deletes id index data for lookup key.
    Args:
        input(Dict): Dictionary containing following keys: lookup_key, id-index-table
        session(Session): AWS boto3 Session

    Returns:

    """
    if session is None:
        session = bossutils.aws.get_session()
    id_index_table = input["id-index-table"]
    lookup_key = input["lookup_key"]
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
        raise DeleteError(scan_resp)

    count = 0
    while scan_resp['Count'] > 0:
        for id in scan_resp["Items"]:
            exclusive_start_key=id
            count += 1
            print("deleting: {}".format(id))
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
            del_resp["deleting"] = id
            raise DeleteError(scan_resp)
    print("deleted {} items".format(count))


def delete_s3_index(input, session=None):
    """
    Deletes s3 index keys containing the lookup key after deleting the S3 Object from cuboid bucket
    Args:
        input(Dict): Dictionary containing following keys: lookup_key, id-count-table
        session(Session): AWS boto3 Session

    Returns:

    """
    s3_index_table = input["s3-index-table"]

    lookup_key = input["lookup_key"]
    channel_key = get_channel_key(lookup_key)
    col, exp, ch = lookup_key.split("&")
    session = bossutils.aws.get_session()
    client = session.client('dynamodb')
    s3client = session.client('s3')
    query_params = {'TableName': s3_index_table,
                    'IndexName': S3_INDEX_TABLE_INDEX,
                    'KeyConditionExpression': '#ingest_job_hash = :ingest_job_hash_value AND begins_with(#ingest_job_range, :ingest_job_range_value)',
                    'ExpressionAttributeValues': {":ingest_job_hash_value": {"S": col}, ":ingest_job_range_value": {"S": exp+"&"+ch}},
                    'ExpressionAttributeNames': {"#object_key": "object-key", "#version_node": "version-node",
                                                 "#ingest_job_hash": "ingest-job-hash", "#ingest_job_range": "ingest-job-range"},
                    'ProjectionExpression': "#object_key, #version_node",
                    'Limit': 10}
    query_resp = client.query(**query_params)
    if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise DeleteError(query_resp)

    count = 0
    while query_resp['Count'] > 0:
        for id in query_resp["Items"]:
            exclusive_start_key=id
            count += 1
            print("deleting: {}".format(id))
            # this is where you would delete the s3 objects.

            del_resp = client.delete_item(
                TableName=s3_index_table,
                Key=id,
                ReturnValues='NONE',
                ReturnConsumedCapacity='NONE')
            if del_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                del_resp["deleting"] = id
                raise DeleteError(del_resp)
        # Keep querying to make sure we have them all.
        query_params['ExclusiveStartKey'] = exclusive_start_key
        query_resp = client.query(**query_params)
        if query_resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            del_resp["deleting"] = id
            raise DeleteError(query_resp)
    print("deleted {} items".format(count))

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
    input["dt3"] = True
    pprint.pprint(input)
    return input


if __name__ == "__main__":
    input_from_main = {
        "lookup_key": "41&29&35",  # was lookup key being used by intTest
        #"lookup_key": "23",  # lookup key being used by metadata
        "meta-db": "bossmeta.hiderrt1.boss",
        "s3-index-table": "s3index.hiderrt1.boss",
        "id-index-table": "idIndex.hiderrt1.boss",
        "id-count-table": "idCount.hiderrt1.boss",
        "cuboid_bucket": "cuboids.hiderrt1.boss",
        #"id-count-table": "intTest.idCount.hiderrt1.boss"  # had data for idCount
    }
    test = "hello"
    #delete_metedata(input_from_main)
    #delete_id_count(input_from_main)
    #delete_id_index(input_from_main)
    delete_s3_index(input_from_main)
    print("done.")
