#!/usr/bin/env python3.4
# This lambda is for s3_flush for the cache
#
# It expects to get from events dictionary
# {
#   "lambda-name": "s3_flush",
#   "cache-state": "cache-state.hiderrt1.boss",
#   "cache-state-db": "0"
#   "s3-flush-queue": "https://sqs.us-east-1.amazonaws.com/<aws_account>/S3flushHiderrt1Boss"
#   "s3-flush-deadletter-queue": "https://sqs.us-east-1.amazonaws.com/<aws_account>/DeadletterHiderrt1Boss"
#   "cuboid-bucket": "cuboids.hiderrt1.boss"
#   "s3-index-table": "s3Index.hiderrt1.boss"
# }
#
# It expects to get from events dictionary
# {
#   "lambda-name": "s3_flush",
#   "config": {'kv_config': {...}
#              'state_config': {...},
#              'object_store_config': {...}},
#   "write_cuboid_key": "...",
#   "resource": {...}
# }
#

import sys
import json
import time
import boto3
import botocore

from spdb.spatialdb import SpatialDB
from spdb.spatialdb import Cube
from spdb.project import BossResourceBasic
from spdb.c_lib.ndtype import CUBOIDSIZE


# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

run_cnt = 0

while run_cnt < 2:
    # Get message from SQS flush queue, try for ~10 seconds with backoff
    sqs_client = boto3.client('sqs')
    rx_cnt = 0
    flush_msg_data = None
    rx_handle = ''
    while rx_cnt < 6:
        try:
            flush_msg = sqs_client.receive_message(QueueUrl=event["config"]["object_store_config"]["s3_flush_queue"])
        except botocore.exceptions.ClientError:
            print("Failed to get message. Trying again...")
            flush_msg = {}
            time.sleep(.5)

        if "Messages" in flush_msg:
            # Get Message
            flush_msg_data = flush_msg['Messages'][0]
            break
        else:
            rx_cnt += 1
            print("No message found. Try {} of 6".format(rx_cnt))
            time.sleep(.25)

    if flush_msg_data:
        # Got a message

        # Get Message Receipt Handle
        rx_handle = flush_msg_data['ReceiptHandle']

        # Load the message body
        flush_msg_data = json.loads(flush_msg_data['Body'])

        print("Message: {}".format(flush_msg_data))

        # Setup SPDB instance
        sp = SpatialDB(flush_msg_data["config"]["kv_config"],
                       flush_msg_data["config"]["state_config"],
                       flush_msg_data["config"]["object_store_config"])

        # Get the write-cuboid key to flush
        write_cuboid_key = flush_msg_data['write_cuboid_key']
        print("Flushing {} to S3".format(write_cuboid_key))

        # Create resource instance
        resource = BossResourceBasic()
        resource.from_dict(flush_msg_data["resource"])
    else:
        # Nothing to flush. Exit.
        sys.exit("No flush message available")

    # Check if cuboid is in S3
    object_keys = sp.objectio.write_cuboid_to_object_keys([write_cuboid_key])
    cache_key = sp.kvio.write_cuboid_key_to_cache_key(write_cuboid_key)
    exist_keys, missing_keys = sp.objectio.cuboids_exist(cache_key)

    print("write key: {}".format(write_cuboid_key))
    print("object key: {}".format(object_keys[0]))
    print("cache key: {}".format(cache_key))

    resolution = int(write_cuboid_key.split('&')[4])
    cube_dim = CUBOIDSIZE[resolution]
    time_sample = int(write_cuboid_key.split('&')[5])
    morton = int(write_cuboid_key.split('&')[6])
    write_cuboid_keys_to_remove = [write_cuboid_key]

    if exist_keys:  # Cuboid Exists
        # Get cuboid to flush from write buffer
        write_cuboid_bytes = sp.kvio.get_cube_from_write_buffer(write_cuboid_key)
        new_cube = Cube.create_cube(resource, cube_dim)
        new_cube.morton_id = morton
        new_cube.from_blosc(write_cuboid_bytes)

        # Get existing cuboid from S3
        existing_cube = Cube.create_cube(resource, cube_dim)
        existing_cube.morton_id = new_cube.morton_id
        existing_cube_bytes = sp.objectio.get_single_object(object_keys[0])
        existing_cube.from_blosc(existing_cube_bytes, new_cube.time_range)

        # Merge cuboids
        existing_cube.overwrite(new_cube.data, new_cube.time_range)

        # Get bytes
        cuboid_bytes = existing_cube.to_blosc()
        uncompressed_cuboid_bytes = existing_cube.data

    else:  # Cuboid Does Not Exist
        # Get cuboid to flush from write buffer
        cuboid_bytes = sp.kvio.get_cube_from_write_buffer(write_cuboid_key)
        new_cube = Cube.create_cube(resource, cube_dim)
        t_range = [time_sample, time_sample+1]
        new_cube.from_blosc(cuboid_bytes, t_range)
        uncompressed_cuboid_bytes = new_cube.data


    # Check for delayed writes for this cuboid
    delayed_writes = sp.cache_state.get_delayed_writes(sp.cache_state.write_cuboid_key_to_delayed_write_key(write_cuboid_key))
    if delayed_writes:
        print("Processing Delayed Writes")
        # Create cube for current data
        existing_cube = Cube.create_cube(resource, cube_dim)
        existing_cube.morton_id = morton
        existing_cube.from_blosc(cuboid_bytes)

        # Collapse all writes into a single op
        for key in delayed_writes:
            print("Delayed Write: {}".format(key))
            # Track what keys have been flushed
            write_cuboid_keys_to_remove.append(key)
            # Get the data from the buffer
            write_cuboid_bytes = sp.kvio.get_cube_from_write_buffer(key)
            new_cube = Cube.create_cube(resource, cube_dim)
            new_cube.morton_id = morton
            new_cube.from_blosc(write_cuboid_bytes)

            # Merge data
            existing_cube.overwrite(new_cube.data, existing_cube.time_range)

        # Update bytes to send to s3 and bytes scanned for ids
        cuboid_bytes = existing_cube.to_blosc()
        uncompressed_cuboid_bytes = existing_cube.data

    # Write cuboid to S3
    sp.objectio.put_objects(object_keys, [cuboid_bytes])

    # Add to S3 Index if this is a new cube
    if not exist_keys:
        sp.objectio.add_cuboid_to_index(object_keys[0])

    # Update id indices if this is an annotation channel
    if resource.data['channel']['type'] == 'annotation':
        sp.objectio.update_id_indices(resource, resolution, [object_keys[0]], [uncompressed_cuboid_bytes])

    # Check if cuboid already exists in the cache
    if sp.kvio.cube_exists(cache_key):
        # It exists. Update with latest data.
        print("CUBOID EXISTS IN CACHE - UPDATING")
        sp.kvio.put_cubes([cache_key], [cuboid_bytes])

    # Delete write-cuboid key
    for key in write_cuboid_keys_to_remove:
        sp.kvio.delete_cube(key)

    # Remove page-out entry
    sp.cache_state.remove_from_page_out(write_cuboid_key)

    # Delete message since it was processed successfully
    sqs_client.delete_message(QueueUrl=event["config"]["object_store_config"]["s3_flush_queue"],
                              ReceiptHandle=rx_handle)

    # Increment run counter
    run_cnt += 1
