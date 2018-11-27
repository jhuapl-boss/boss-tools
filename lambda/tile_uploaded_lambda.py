# Copyright 2014 NeuroData (http://neurodata.io)
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

from bossnames.names import AWSNames
import boto3
import json
from ndingest.settings.bosssettings import BossSettings
import urllib

from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.ndingestproj.bossingestproj import BossIngestProj

from botocore.exceptions import ClientError

def handler(event, context):
    # Load settings
    SETTINGS = BossSettings.load()

    # extract bucket name and tile key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    tile_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    print("Bucket: {}".format(bucket))
    print("Tile key: {}".format(tile_key))

    # fetch metadata from the s3 object
    proj_info = BossIngestProj.fromTileKey(tile_key)
    tile_bucket = TileBucket(proj_info.project_name)
    message_id, receipt_handle, metadata = tile_bucket.getMetadata(tile_key)
    print("Metadata: {}".format(metadata))

    # Currently this is what is sent from the client for the "metadata"
    #  metadata = {'chunk_key': 'chunk_key',
    #              'ingest_job': self.ingest_job_id,
    #              'parameters': {"upload_queue": XX
    #                             "ingest_queue": XX,
    #                             "ingest_lambda":XX,
    #                             "KVIO_SETTINGS": XX,
    #                             "STATEIO_CONFIG": XX,
    #                             "OBJECTIO_CONFIG": XX
    #                             },
    #              'tile_size_x': "{}".format(self.config.config_data["ingest_job"]["tile_size"]["x"]),
    #              'tile_size_y': "{}".format(self.config.config_data["ingest_job"]["tile_size"]["y"])
    #              }

    # TODO: DMK not sure if you actually need to set the job_id in proj_info
    # Set the job id
    proj_info.job_id = metadata["ingest_job"]

    # update value in the dynamo table
    tile_index_db = BossTileIndexDB(proj_info.project_name)
    chunk = tile_index_db.getCuboid(metadata["chunk_key"], int(metadata["ingest_job"]))
    if chunk:
        if tile_index_db.cuboidReady(metadata["chunk_key"], chunk["tile_uploaded_map"]):
            print("Chunk already has all its tiles: {}".format(metadata["chunk_key"]))
            # Go ahead and setup to fire another ingest lambda so this tile
            # entry will be deleted on successful execution of the ingest lambda.
            chunk_ready = True
        else:
            print("Updating tile index for chunk_key: {}".format(metadata["chunk_key"]))
            chunk_ready = tile_index_db.markTileAsUploaded(metadata["chunk_key"], tile_key, int(metadata["ingest_job"]))
    else:
        # First tile in the chunk
        print("Creating first entry for chunk_key: {}".format(metadata["chunk_key"]))
        try:
            tile_index_db.createCuboidEntry(metadata["chunk_key"], int(metadata["ingest_job"]))
        except ClientError as err:
            # Under _exceptional_ circumstances, it's possible for another lambda
            # to beat the current instance to creating the initial cuboid entry
            # in the index.
            error_code = err.response['Error'].get('Code', 'Unknown')
            if error_code == 'ConditionalCheckFailedException':
                print('Chunk key entry already created - proceeding.')
            else:
                raise
        chunk_ready = tile_index_db.markTileAsUploaded(metadata["chunk_key"], tile_key, int(metadata["ingest_job"]))

    # ingest the chunk if we have all the tiles
    if chunk_ready:
        print("CHUNK READY SENDING MESSAGE: {}".format(metadata["chunk_key"]))
        # insert a new job in the insert queue if we have all the tiles
        ingest_queue = IngestQueue(proj_info)
        ingest_queue.sendMessage(json.dumps(metadata))

        # Invoke Ingest lambda function

        names = AWSNames.create_from_lambda_name(context.function_name)
        lambda_client = boto3.client('lambda', region_name=SETTINGS.REGION_NAME)
        lambda_client.invoke(
            FunctionName=names.tile_ingest_lambda,
            InvocationType='Event',
            Payload=json.dumps(metadata).encode())
    else:
        print("Chunk not ready for ingest yet: {}".format(metadata["chunk_key"]))

    # Delete message from upload queue
    upload_queue = UploadQueue(proj_info)
    upload_queue.deleteMessage(message_id, receipt_handle)
    print("DONE!")
