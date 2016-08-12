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
from spdb.c_lib.ndtype import CUBOIDSIZE


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
rx_handle = ''
while rx_cnt < 6:
    flush_msg = sqs_client.receive_message(QueueUrl=event["config"]["object_store_config"]["s3_flush_queue"])
    if len(flush_msg["Messages"]) > 0:
        # Get Message
        flush_msg_data = flush_msg['Messages'][0]
        break
    else:
        rx_cnt += 1
        print("No message found. Try {} of 6".format(rx_cnt))
        time.sleep(.5 * rx_cnt)

if flush_msg_data:
    # Got a message

    # Get Message Receipt Handle
    rx_handle = flush_msg_data['ReceiptHandle']

    # Load the message body
    flush_msg_data = json.loads(flush_msg_data['Body'])

    # Get the write-cuboid key to flush
    write_cuboid_key = flush_msg_data['write_cuboid_key']
    print("Flushing {} to S3".format(write_cuboid_key))

    # Get the resource
    resource = flush_msg_data['resource']
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

if exist_keys:  # Cuboid Exists
    # Create resource instance
    resource = BossResourceBasic()
    resource.from_dict(flush_msg_data["resource"])
    cube_dim = CUBOIDSIZE[int(write_cuboid_key.split('&')[4])]
    morton = int(write_cuboid_key.split('&')[6])

    # Get cuboid to flush from write buffer
    write_cuboid_bytes = sp.kvio.get_cube_from_write_buffer(write_cuboid_key)
    new_cube = Cube.create_cube(resource, cube_dim)
    new_cube.morton_id = morton
    new_cube.from_blosc_numpy(write_cuboid_bytes)

    # Get existing cuboid from S3
    existing_cube = Cube.create_cube(resource, cube_dim)
    existing_cube.morton_id = new_cube.morton_id
    existing_cube_bytes = sp.objectio.get_single_object(object_keys[0])
    existing_cube.from_blosc_numpy(existing_cube_bytes, new_cube.time_range)

    # Merge cuboids
    existing_cube.overwrite(new_cube.data, new_cube.time_range)

    # Get bytes
    cuboid_bytes = existing_cube.to_blosc_numpy()

else:  # Cuboid Does Not Exist
    # Get cuboid to flush from write buffer
    cuboid_bytes = sp.kvio.get_cube_from_write_buffer(write_cuboid_key)


# Write cuboid to S3
sp.objectio.put_objects(object_keys, [cuboid_bytes])

# Add to S3 Index
if not exist_keys:
    sp.objectio.add_cuboid_to_index(object_keys[0])

# Check if cuboid already exists in the cache
if sp.kvio.cube_exists(cache_key):
    # It exists. Update with latest data.
    print("CUBOID EXISTS IN CACHE - UPDATING")
    sp.kvio.put_cubes([cache_key], [cuboid_bytes])

# Delete write-cuboid key
sp.kvio.delete_cube(write_cuboid_key)

# Remove page-out entry
sp.cache_state.remove_from_page_out(write_cuboid_key)

# Delete message since it was processed successfully
sqs_client.delete_message(QueueUrl=event["config"]["object_store_config"]["s3_flush_queue"],
                          ReceiptHandle=rx_handle)
