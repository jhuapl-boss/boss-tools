# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from ndingest.ndqueue.tileerrorqueue import TileErrorQueue
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.util.bossutil import BossUtil

from bossnames.names import AWSNames

from io import BytesIO
from PIL import Image
import numpy as np
import math
import boto3


def handler(event, context):
    # Load settings
    SETTINGS = BossSettings.load()

    # Used as a guard against trying to delete the SQS message when lambda is
    # triggered by SQS.
    sqs_triggered = 'Records' in event and len(event['Records']) > 0

    if sqs_triggered :
        # Lambda invoked by an SQS trigger.
        msg_data = json.loads(event['Records'][0]['body'])
        # Load the project info from the chunk key you are processing
        chunk_key = msg_data['chunk_key']
        proj_info = BossIngestProj.fromSupercuboidKey(chunk_key)
        proj_info.job_id = msg_data['ingest_job']
    else:
        # Standard async invoke of this lambda.

        # Load the project info from the chunk key you are processing
        proj_info = BossIngestProj.fromSupercuboidKey(event["chunk_key"])
        proj_info.job_id = event["ingest_job"]

        # Get message from SQS ingest queue, try for ~2 seconds
        rx_cnt = 0
        msg_data = None
        msg_id = None
        msg_rx_handle = None
        while rx_cnt < 6:
            ingest_queue = IngestQueue(proj_info)
            try:
                msg = [x for x in ingest_queue.receiveMessage()]
            # StopIteration may be converted to a RunTimeError.
            except (StopIteration, RuntimeError):
                msg = None

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
            # No tiles ready to ingest.
            print("No ingest message available")
            return

        # Get the chunk key of the tiles to ingest.
        chunk_key = msg_data['chunk_key']


    tile_error_queue = TileErrorQueue(proj_info)

    print("Ingesting Chunk {}".format(chunk_key))
    tiles_in_chunk = int(chunk_key.split('&')[1])

    # Setup SPDB instance
    sp = SpatialDB(msg_data['parameters']["KVIO_SETTINGS"],
                   msg_data['parameters']["STATEIO_CONFIG"],
                   msg_data['parameters']["OBJECTIO_CONFIG"])

    # Get tile list from Tile Index Table
    tile_index_db = BossTileIndexDB(proj_info.project_name)
    # tile_index_result (dict): keys are S3 object keys of the tiles comprising the chunk.
    tile_index_result = tile_index_db.getCuboid(msg_data["chunk_key"], int(msg_data["ingest_job"]))
    if tile_index_result is None:
        # If chunk_key is gone, another lambda uploaded the cuboids and deleted the chunk_key afterwards.
        if not sqs_triggered:
            # Remove message so it's not redelivered.
            ingest_queue.deleteMessage(msg_id, msg_rx_handle)

        print("Aborting due to chunk key missing from tile index table")
        return

    # Sort the tile keys
    print("Tile Keys: {}".format(tile_index_result["tile_uploaded_map"]))
    tile_key_list = [x.rsplit("&", 2) for x in tile_index_result["tile_uploaded_map"].keys()]
    if len(tile_key_list) < tiles_in_chunk:
        print("Not a full set of 16 tiles. Assuming it has handled already, tiles: {}".format(len(tile_key_list)))
        if not sqs_triggered:
            ingest_queue.deleteMessage(msg_id, msg_rx_handle)
        return
    tile_key_list = sorted(tile_key_list, key=lambda x: int(x[1]))
    tile_key_list = ["&".join(x) for x in tile_key_list]
    print("Sorted Tile Keys: {}".format(tile_key_list))

    # Augment Resource JSON data so it will instantiate properly that was pruned due to S3 metadata size limits
    resource_dict = msg_data['parameters']['resource']
    _, exp_name, ch_name = resource_dict["boss_key"].split("&")

    resource_dict["channel"]["name"] = ch_name
    resource_dict["channel"]["description"] = ""
    resource_dict["channel"]["sources"] = []
    resource_dict["channel"]["related"] = []
    resource_dict["channel"]["default_time_sample"] = 0
    resource_dict["channel"]["downsample_status"] = "NOT_DOWNSAMPLED"

    resource_dict["experiment"]["name"] = exp_name
    resource_dict["experiment"]["description"] = ""
    resource_dict["experiment"]["num_time_samples"] = 1
    resource_dict["experiment"]["time_step"] = None
    resource_dict["experiment"]["time_step_unit"] = None

    resource_dict["coord_frame"]["name"] = "cf"
    resource_dict["coord_frame"]["name"] = ""
    resource_dict["coord_frame"]["x_start"] = 0
    resource_dict["coord_frame"]["x_stop"] = 100000
    resource_dict["coord_frame"]["y_start"] = 0
    resource_dict["coord_frame"]["y_stop"] = 100000
    resource_dict["coord_frame"]["z_start"] = 0
    resource_dict["coord_frame"]["z_stop"] = 100000
    resource_dict["coord_frame"]["voxel_unit"] = "nanometers"

    # Setup the resource
    resource = BossResourceBasic()
    resource.from_dict(resource_dict)
    dtype = resource.get_numpy_data_type()

    # read all tiles from bucket into a slab
    tile_bucket = TileBucket(proj_info.project_name)
    data = []
    num_z_slices = 0
    for tile_key in tile_key_list:
        try:
            image_data, message_id, receipt_handle, metadata = tile_bucket.getObjectByKey(tile_key)
        except KeyError:
            print('Key: {} not found in tile bucket, assuming redelivered SQS message and aborting.'.format(
                tile_key))
            if not sqs_triggered:
                # Remove message so it's not redelivered.
                ingest_queue.deleteMessage(msg_id, msg_rx_handle)
            print("Aborting due to missing tile in bucket")
            return

        image_bytes = BytesIO(image_data)
        image_size = image_bytes.getbuffer().nbytes

        # Get tiles size from metadata, need to shape black tile if actual tile is corrupt.
        if 'x_size' in metadata:
            tile_size_x = metadata['x_size']
        else:
            print('MetadataMissing: x_size not in tile metadata:  using 1024.')
            tile_size_x = 1024

        if 'y_size' in metadata:
            tile_size_y = metadata['y_size']
        else:
            print('MetadataMissing: y_size not in tile metadata:  using 1024.')
            tile_size_y = 1024

        if image_size == 0:
            print('TileError: Zero length tile, using black instead: {}'.format(tile_key))
            error_msg = 'Zero length tile'
            enqueue_tile_error(tile_error_queue, tile_key, chunk_key, error_msg)
            tile_img = np.zeros((tile_size_x, tile_size_y), dtype=dtype)
        else:
            try:
                tile_img = np.asarray(Image.open(image_bytes), dtype=dtype)
            except TypeError as te:
                print('TileError: Incomplete tile, using black instead (tile_size_in_bytes, tile_key): {}, {}'
                      .format(image_size, tile_key))
                error_msg = 'Incomplete tile'
                enqueue_tile_error(tile_error_queue, tile_key, chunk_key, error_msg)
                tile_img = np.zeros((tile_size_x, tile_size_y), dtype=dtype)
            except OSError as oe:
                print('TileError: OSError, using black instead (tile_size_in_bytes, tile_key): {}, {} ErrorMessage: {}'
                      .format(image_size, tile_key, oe))
                error_msg = 'OSError: {}'.format(oe)
                enqueue_tile_error(tile_error_queue, tile_key, chunk_key, error_msg)
                tile_img = np.zeros((tile_size_x, tile_size_y), dtype=dtype)

        data.append(tile_img)
        num_z_slices += 1


    # Make 3D array of image data. It should be in XYZ at this point
    chunk_data = np.array(data)
    del data
    tile_dims = chunk_data.shape

    # Break into Cube instances
    print("Tile Dims: {}".format(tile_dims))
    print("Num Z Slices: {}".format(num_z_slices))
    num_x_cuboids = int(math.ceil(tile_dims[2] / CUBOIDSIZE[proj_info.resolution][0]))
    num_y_cuboids = int(math.ceil(tile_dims[1] / CUBOIDSIZE[proj_info.resolution][1]))

    print("Num X Cuboids: {}".format(num_x_cuboids))
    print("Num Y Cuboids: {}".format(num_y_cuboids))

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
            # We no longer index during ingest.
            #if resource.data['channel']['type'] == 'annotation':
            #   try:
            #       sp.objectio.update_id_indices(
            #           resource, proj_info.resolution, [object_key], [cube.data])
            #   except SpdbError as ex:
            #       sns_client = boto3.client('sns')
            #       topic_arn = msg_data['parameters']["OBJECTIO_CONFIG"]["prod_mailing_list"]
            #       msg = 'During ingest:\n{}\nCollection: {}\nExperiment: {}\n Channel: {}\n'.format(
            #           ex.message,
            #           resource.data['collection']['name'],
            #           resource.data['experiment']['name'],
            #           resource.data['channel']['name'])
            #       sns_client.publish(
            #           TopicArn=topic_arn,
            #           Subject='Object services misuse',
            #           Message=msg)

    lambda_client = boto3.client('lambda', region_name=SETTINGS.REGION_NAME)

    names = AWSNames.from_lambda(context.function_name)

    delete_tiles_data = {
        'tile_key_list': tile_key_list,
        'region': SETTINGS.REGION_NAME,
        'bucket': tile_bucket.bucket.name
    }

    # Delete tiles from tile bucket.
    lambda_client.invoke(
        FunctionName=names.delete_tile_objs.lambda_,
        InvocationType='Event',
        Payload=json.dumps(delete_tiles_data).encode()
    )       

    delete_tile_entry_data = {
        'tile_index': tile_index_db.table.name,
        'region': SETTINGS.REGION_NAME,
        'chunk_key': chunk_key,
        'task_id': msg_data['ingest_job']
    }

    # Delete entry from tile index.
    lambda_client.invoke(
        FunctionName=names.delete_tile_index_entry.lambda_,
        InvocationType='Event',
        Payload=json.dumps(delete_tile_entry_data).encode()
    )       

    if not sqs_triggered:
        # Delete message since it was processed successfully
        ingest_queue.deleteMessage(msg_id, msg_rx_handle)


def enqueue_tile_error(queue, tile_key, chunk_key, error_msg):
    """
    Send info about bad tile to the tile error queue.

    Args:
        queue (TileErrorQueue):
        tile_key (str):
        chunk_key (str):
        error_msg (str):
    """
    msg = {'tile_key': tile_key, 'chunk_key': chunk_key,  'error': error_msg }
    queue.sendMessage(json.dumps(msg))
