#!/usr/bin/env python3.4
# This lambda is for ingest

import sys
import json
import time

from spdb.spatialdb import Cube, SpatialDB, SpdbError
from spdb.project import BossResourceBasic
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.c_lib.ndlib import XYZMorton

from ndingest.settings.bosssettings import BossSettings
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.util.bossutil import BossUtil

from io import BytesIO
from PIL import Image
import numpy as np
import math
import boto3

print("$$$ IN INGEST LAMBDA $$$")
# Load settings
SETTINGS = BossSettings.load()

# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

# Load the project info from the chunk key you are processing
proj_info = BossIngestProj.fromSupercuboidKey(event["chunk_key"])
proj_info.job_id = event["ingest_job"]

# Handle up to 2 messages before quitting (helps deal with making sure all messages get processed)
run_cnt = 0
while run_cnt < 2:
    # Get message from SQS flush queue, try for ~2 seconds
    rx_cnt = 0
    msg_data = None
    msg_id = None
    msg_rx_handle = None
    while rx_cnt < 6:
        ingest_queue = IngestQueue(proj_info)
        msg = [x for x in ingest_queue.receiveMessage()]
        if msg:
            msg = msg[0]
            print("MESSAGE: {}".format(msg))
            print(len(msg))
            msg_id = msg[0]
            msg_rx_handle = msg[1]
            msg_data = json.loads(msg[2])
            print("MESSAGE DATA: {}".format(msg_data))
            break
        else:
            rx_cnt += 1
            print("No message found. Try {} of 6".format(rx_cnt))
            time.sleep(1)

    if not msg_id:
        # Nothing to flush. Exit.
        sys.exit("No ingest message available")

    # Get the write-cuboid key to flush
    chunk_key = msg_data['chunk_key']
    print("Ingesting Chunk {}".format(chunk_key))

    # Setup SPDB instance
    sp = SpatialDB(msg_data['parameters']["KVIO_SETTINGS"],
                   msg_data['parameters']["STATEIO_CONFIG"],
                   msg_data['parameters']["OBJECTIO_CONFIG"])

    # Get tile list from Tile Index Table
    tile_index_db = BossTileIndexDB(proj_info.project_name)
    # tile_index_result (dict): keys are S3 object keys of the tiles comprising the chunk.
    tile_index_result = tile_index_db.getCuboid(msg_data["chunk_key"], int(msg_data["ingest_job"]))
    if tile_index_result is None:
        # Remove message so it's not redelivered.
        ingest_queue.deleteMessage(msg_id, msg_rx_handle)
        sys.exit("Aborting due to chunk key missing from tile index table")

    # Sort the tile keys
    print("Tile Keys: {}".format(tile_index_result["tile_uploaded_map"]))
    tile_key_list = [x.rsplit("&", 2) for x in tile_index_result["tile_uploaded_map"].keys()]
    tile_key_list = sorted(tile_key_list, key=lambda x: int(x[1]))
    tile_key_list = ["&".join(x) for x in tile_key_list]
    print("Sorted Tile Keys: {}".format(tile_key_list))

    # Setup the resource
    resource = BossResourceBasic()
    resource.from_dict(msg_data['parameters']['resource'])
    dtype = resource.get_numpy_data_type()

    # read all tiles from bucket into a slab
    tile_bucket = TileBucket(proj_info.project_name)
    data = []
    num_z_slices = 0
    for tile_key in tile_key_list:
        try:
            image_data, message_id, receipt_handle, _ = tile_bucket.getObjectByKey(tile_key)
        except KeyError:
            print('Key: {} not found in tile bucket, assuming redelivered SQS message and aborting.'.format(
                tile_key))
            # Remove message so it's not redelivered.
            ingest_queue.deleteMessage(msg_id, msg_rx_handle)
            sys.exit("Aborting due to missing tile in bucket")

        tile_img = np.asarray(Image.open(BytesIO(image_data)), dtype=dtype)
        #tile_img = np.swapaxes(tile_img, 0, 1)
        data.append(tile_img)
        num_z_slices += 1
        # TODO Make sure data type is correct

    # Make 3D array of image data. It should be in XYZ at this point
    chunk_data = np.array(data)
    del data
    # TODO: Make sure data is not transposed
    tile_dims = chunk_data.shape

    # Break into Cube instances
    print("Tile Dims: {}".format(tile_dims))
    print("Num Z Slices: {}".format(num_z_slices))
    num_x_cuboids = int(math.ceil(tile_dims[2] / CUBOIDSIZE[proj_info.resolution][0]))
    num_y_cuboids = int(math.ceil(tile_dims[1] / CUBOIDSIZE[proj_info.resolution][1]))

    print("Num X Cuboids: {}".format(num_x_cuboids))
    print("Num Y Cuboids: {}".format(num_y_cuboids))

    # Cuboid List
    cuboids = []
    chunk_key_parts = BossUtil.decode_chunk_key(chunk_key)
    t_index = chunk_key_parts['t_index']
    for x_idx in range(0, num_x_cuboids):
        for y_idx in range(0, num_y_cuboids):
            # TODO: check time series support
            cube = Cube.create_cube(resource, CUBOIDSIZE[proj_info.resolution])
            cube.zeros()

            # Compute Morton ID
            # TODO: verify Morton indices correct!
            print(chunk_key_parts)
            morton_x_ind = x_idx + (chunk_key_parts["x_index"] * num_x_cuboids)
            morton_y_ind = y_idx + (chunk_key_parts["y_index"] * num_y_cuboids)
            print("Morton X: {}".format(morton_x_ind))
            print("Morton Y: {}".format(morton_y_ind))
            morton_index = XYZMorton([morton_x_ind, morton_y_ind, int(chunk_key_parts['z_index'])])

            # Insert sub-region from chunk_data into cuboid
            x_start = x_idx * CUBOIDSIZE[proj_info.resolution][0]
            x_end = x_start + CUBOIDSIZE[proj_info.resolution][0]
            x_end = min(x_end, tile_dims[2])
            y_start = y_idx * CUBOIDSIZE[proj_info.resolution][1]
            y_end = y_start + CUBOIDSIZE[proj_info.resolution][1]
            y_end = min(y_end, tile_dims[1])
            z_end = CUBOIDSIZE[proj_info.resolution][2]
            # TODO: get sub-array w/o making a copy.
            print("Yrange: {}".format(y_end - y_start))
            print("Xrange: {}".format(x_end - x_start))
            print("X start: {}".format(x_start))
            print("X stop: {}".format(x_end))
            cube.data[0, 0:num_z_slices, 0:(y_end - y_start), 0:(x_end - x_start)] = chunk_data[0:num_z_slices,
                                                                                 y_start:y_end, x_start:x_end]

            # Create object key
            object_key = sp.objectio.generate_object_key(resource, proj_info.resolution, t_index, morton_index)
            print("Object Key: {}".format(object_key))

            # Put object in S3
            sp.objectio.put_objects([object_key], [cube.to_blosc()])

            # Add object to index
            sp.objectio.add_cuboid_to_index(object_key, ingest_job=int(msg_data["ingest_job"]))

            # Update id indices if this is an annotation channel
            if resource.data['channel']['type'] == 'annotation':
                try:
                    sp.objectio.update_id_indices(
                        resource, proj_info.resolution, [object_key], [cube.data])
                except SpdbError as ex:
                    sns_client = boto3.client('sns')
                    topic_arn = msg_data['parameters']["OBJECTIO_CONFIG"]["prod_mailing_list"]
                    msg = 'During ingest:\n{}\nCollection: {}\nExperiment: {}\n Channel: {}\n'.format(
                        ex.message,
                        resource.data['collection']['name'],
                        resource.data['experiment']['name'],
                        resource.data['channel']['name'])
                    sns_client.publish(
                        TopicArn=topic_arn,
                        Subject='Object services misuse',
                        Message=msg)


    # Delete message since it was processed successfully
    ingest_queue.deleteMessage(msg_id, msg_rx_handle)

    # Delete Tiles
    for tile in tile_key_list:
        for try_cnt in range(0, 4):
            try:
                time.sleep(try_cnt)
                print("Deleting tile: {}".format(tile))
                tile_bucket.deleteObject(tile)
                break
            except:
                print("failed")

    # Delete Entry in tile table
    for try_cnt in range(0, 4):
        try:
            time.sleep(try_cnt)
            tile_index_db.deleteCuboid(chunk_key, int(msg_data["ingest_job"]))
            break
        except:
            print("failed")

    # Increment run counter
    run_cnt += 1
