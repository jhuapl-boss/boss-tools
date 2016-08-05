#!/usr/bin/env python3.4
# This lambda is for s3_flush for the cache
#
# It expects to get from events dictionary
# {
#   "lambda-name": "s3_flush",
#   "cache-state": "cache-state.hiderrt1.boss",
#   "cache-state-db": "0"
#   "s3-flush-queue": "https://sqs.us-east-1.amazonaws.com/256215146792/S3flushHiderrt1Boss"
#   "s3-flush-deadletter-queue": "https://sqs.us-east-1.amazonaws.com/256215146792/DeadletterHiderrt1Boss"
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

from spdb.spatialdb import SpatialDB
from spdb.spatialdb import Cube
from spdb.project import BossResourceBasic


# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

# Setup SPDB instance
sp = SpatialDB(event["config"]["kv_config"],
               event["config"]["state_config"],
               event["config"]["object_store_config"])

# Get message from SQS flush queue, try for ~10 seconds with backoff
sqs_client = boto3.client('sqs')
rx_cnt = 0
flush_msg_data = None
while rx_cnt < 6:
    flush_msg = sqs_client.receive_message(QueueUrl=event["s3-flush-queue"])
    if len(flush_msg["Messages"]) > 0:
        flush_msg_data = json.loads(flush_msg['Messages'][0])
        break
    else:
        rx_cnt += 1
        time.sleep(.5 * rx_cnt)

if flush_msg_data:
    # Got a message. Get key you are flushing
    write_cuboid_key = flush_msg_data['write_cuboid_key']
else:
    # Nothing to flush. Exit.
    sys.exit("no flush message available")

# Check if cuboid is in S3
object_keys = sp.objectio.cached_cuboid_to_object_keys([write_cuboid_key])
exist_keys, missing_keys = sp.objectio.cuboids_exist(object_keys)

if exist_keys:  # Cuboid Exists
    # Create resource instance
    resource = BossResourceBasic()
    resource.from_json(event["resource"])

    # Get cuboid to flush from write buffer
    new_cube = sp.get_cubes(resource, write_cuboid_key)[0]

    # Get existing cuboid from S3
    existing_cube = Cube.create_cube(resource)
    existing_cube.morton_id = new_cube.morton_id
    existing_cube_bytes = sp.objectio.get_single_object(exist_keys[0])
    existing_cube.from_blosc_numpy(existing_cube_bytes, new_cube.time_range)

    # Merge cuboids
    existing_cube.overwrite(new_cube.data, new_cube.time_range)

    # Write cuboid to S3
    sp.objectio.put_objects(exist_keys, existing_cube.to_blosc_numpy())

    # Add to S3 Index
    sp.objectio.add_cuboid_to_index(exist_keys[0])

    cache_key = sp.kvio.write_cuboid_key_to_cache_key(exist_keys[0])

else:  # Cuboid Does Not Exist
    # Get cuboid to flush from write buffer
    cuboid_bytes = sp.kvio.get_cubes(missing_keys)

    # Write cuboid to S3
    sp.objectio.put_objects(missing_keys, cuboid_bytes)

    # Add to S3 Index
    sp.objectio.add_cuboid_to_index(missing_keys[0])

    cache_key = sp.kvio.write_cuboid_key_to_cache_key(missing_keys[0])


# Check if cuboid already exists in the cache
if sp.kvio.cube_exists(cache_key):
    # It exists. Update with latest data.
    sp.kvio.put_cubes([cache_key], [cuboid_bytes])


# Delete write-cuboid key
sp.kvio.delete_cube(write_cuboid_key)

# Remove page-out entry
sp.cache_state.remove_from_page_out(write_cuboid_key)

# Delete message since it was processed successfully
sqs_client.delete_message(QueueUrl=event["s3-flush-queue"],
                          ReceiptHandle=flush_msg_data["ReceiptHandle"])
