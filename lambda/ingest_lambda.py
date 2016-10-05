#!/usr/bin/env python3.4
# This lambda is for ingest

import sys
import json
import time
import boto3

from spdb.spatialdb import SpatialDB
from spdb.spatialdb import Cube
from spdb.project import BossResourceBasic
from spdb.c_lib.ndtype import CUBOIDSIZE

from ndingest.settings.bosssettings import BossSettings
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.ndqueue.ingestqueue import IngestQueue

# Load settings
SETTINGS = BossSettings.load()

# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

# Load the project info from the chunk key you are processing
proj_info = BossIngestProj.fromSupercuboidKey(event["chunk_key"])

# Handle up to 2 messages before quitting (helps deal with making sure all messages get processed)
run_cnt = 0
while run_cnt < 2:
    # Get message from SQS flush queue, try for ~2 seconds
    rx_cnt = 0
    msg_data = None
    while rx_cnt < 4:
        ingest_queue = IngestQueue(proj_info, endpoint_url=SETTINGS.SQS_ENDPOINT)
        msg = [x for x in ingest_queue.receiveMessage()]
        if msg:
            msg_id = msg[0]
            msg_rx_handle = msg[1]
            msd_data = json.loads(msg[2])
            break
        else:
            rx_cnt += 1
            print("No message found. Try {} of 4".format(rx_cnt))
            time.sleep(.25)

    if not msg_data:
        # Nothing to flush. Exit.
        sys.exit("No flush message available")

    # Create resource instance
    # TODO: HOW DO WE GET THE RESOURCE INFO?
    resource = BossResourceBasic()
    resource.from_dict(msg_data["resource"])

    # Get the write-cuboid key to flush
    chunk_key = msg_data['chunk_key']
    print("Ingesting Chunk {}".format(chunk_key))

    # Setup SPDB instance
    sp = SpatialDB(event["params"]["KVIO_SETTINGS"],
                   event["params"]["STATEIO_CONFIG"],
                   event["params"]["OBJECTIO_CONFIG"])

    # TODO Get tile list from Tile Index Table
    tile_bucket = TileBucket(nd_proj.project_name, endpoint_url=settings.S3_ENDPOINT)
    # read all 64 tiles from bucket into a slab
    for z_index in range(z_tile, settings.SUPER_CUBOID_SIZE[2], 1):
        try:
            image_data, message_id, receipt_handle = tile_bucket.getObject(nd_proj.channel_name, nd_proj.resolution,
                                                                           x_tile, y_tile, z_index, time_index)
            cube.data[z_index, :, :] = np.asarray(Image.open(cStringIO.StringIO(image_data)))
        except Exception as e:
            print("Executing exception")
            pass

    # TODO Load Tiles

    # TODO Break into cuboids

    # Check if cuboid is in S3
    object_keys = sp.objectio.write_cuboid_to_object_keys([write_cuboid_key])
    cache_key = sp.kvio.write_cuboid_key_to_cache_key(write_cuboid_key)
    exist_keys, missing_keys = sp.objectio.cuboids_exist(cache_key)

    print("write key: {}".format(write_cuboid_key))
    print("object key: {}".format(object_keys[0]))
    print("cache key: {}".format(cache_key))

    cube_dim = CUBOIDSIZE[int(write_cuboid_key.split('&')[4])]
    morton = int(write_cuboid_key.split('&')[6])
    write_cuboid_keys_to_remove = [write_cuboid_key]

    # TODO Get Bytes for cube

    # Write cuboids to S3
    sp.objectio.put_objects(object_keys, [cuboid_bytes])

    # Add to S3 Index for all cuboids
    for ok in object_keys:
        sp.objectio.add_cuboid_to_index(ok)

    # TODO Delete Tiles

    # TODO Delete Entry in tile table

    # Delete message since it was processed successfully
    sqs_client.delete_message(QueueUrl=event["config"]["object_store_config"]["s3_flush_queue"],
                              ReceiptHandle=msg_rx_handle)

    # Increment run counter
    run_cnt += 1
