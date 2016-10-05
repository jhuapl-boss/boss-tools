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
from ndingest.nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.ndbucket.tilebucket import TileBucket

from io import StringIO
from PIL import Image
import numpy as np

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
    msg_id = None
    msg_rx_handle = None
    while rx_cnt < 4:
        ingest_queue = IngestQueue(proj_info)
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

    # Get the write-cuboid key to flush
    chunk_key = msg_data['chunk_key']
    print("Ingesting Chunk {}".format(chunk_key))

    # Setup SPDB instance
    sp = SpatialDB(event["params"]["KVIO_SETTINGS"],
                   event["params"]["STATEIO_CONFIG"],
                   event["params"]["OBJECTIO_CONFIG"])

    # Get tile list from Tile Index Table
    tile_index_db = BossTileIndexDB(proj_info.project_name)
    # tile_index_result (dict): keys are S3 object keys of the tiles comprising the chunk.
    tile_index_result = tile_index_db.getCuboid(msg_data["chunk_key"])

    # Sort the tile keys
    print("Tile Keys: {}".format(tile_index_result["tile_uploaded_map"]))
    tile_key_list = sorted(tile_index_result["tile_uploaded_map"])

    # read all tiles from bucket into a slab
    tile_bucket = TileBucket(proj_info.project_name)
    # make this a numpy 3d matrix using numpy to convert list of matrixes to 3d matrix
    data = []
    tile_dims = None
    num_z_slices = 0
    for tile_key in tile_key_list:
        image_data, message_id, receipt_handle, _ = tile_bucket.getObjectByKey(tile_key)
        tile_img = np.asarray(Image.open(StringIO(image_data)))
        tile_dims = tile_img.shape
        data.append(tile_dims)
        num_z_slices += 1
        # TODO Make sure data type is correct

    # Break into Cube instances
    print("Tile Dims: {}".format(tile_dims))
    print("Num Z Slices: {}".format(num_z_slices))
    num_x_cuboids = tile_dims[0] / CUBOIDSIZE[proj_info.resolution][0]
    num_y_cuboids = tile_dims[1] / CUBOIDSIZE[proj_info.resolution][1]

    # Cuboid List
    cuboids = []
    for x_idx in num_x_cuboids:
        for y_idx in num_y_cuboids:
            # Create cube - need boss resource like object
            # check time series support
            cube = Cube.create_cube(, CUBOIDSIZE[proj_info.resolution], [])
            cube.zeros()

            # Compute Morton ID
            # convert tile indices in chunk key to morton tile indices - multiple tile index by num_x_cuboids, etc

            # Insert sub-region from data into cuboid

    cuboidindex_db.putItem(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
    cuboid_bucket.putObject(nd_proj.channel_name, nd_proj.resolution, morton_index, cube.toBlosc())

    # remove message from ingest queue

    # TODO Delete Tiles

    # TODO Delete Entry in tile table

    # Delete message since it was processed successfully
    ingest_queue = IngestQueue(proj_info)
    ingest_queue.deleteMessage(msg_id, msg_rx_handle)

    # Increment run counter
    run_cnt += 1
