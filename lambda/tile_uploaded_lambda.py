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

"""
This lambda updates the tile index DynamoDB table after a tile is uploaded to
S3.  This lambda is triggered by an SQS event from the tile index queue.
After updating the tile's chunk entry in the tile index, it also checks if
the chunk entry now has all of its tiles (normally 16).  If all tiles are
present, it adds the chunk to the ingest queue and invokes the tile ingest
lambda asynchronously.

Lambda may accept multiple SQS messages if desired.

Expected contents of the SQS message
(see ingest-client/ingestclient/core/engine.py for code that creates the message):

{
    chunk_key': 'chunk_key',
    'ingest_job': self.ingest_job_id,
    'parameters': {
        "upload_queue": XX
        "ingest_queue": XX,
        "ingest_lambda":XX,
        "KVIO_SETTINGS": XX,
        "STATEIO_CONFIG": XX,
        "OBJECTIO_CONFIG": XX
    },
    'tile_size_x': "{}".format(self.config.config_data["ingest_job"]["tile_size"]["x"]),
    'tile_size_y': "{}".format(self.config.config_data["ingest_job"]["tile_size"]["y"])
}
"""

from bossnames.names import AWSNames
import boto3
import json
from ndingest.settings.bosssettings import BossSettings

from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingest.ndingestproj.bossingestproj import BossIngestProj

from botocore.exceptions import ClientError

def handler(event, context):
    """
    Lambda entry point.

    Args:
        event (dict): Lambda input parameters.
        context (Context): Lambda context object.
    """
    # Load ndingest settings
    SETTINGS = BossSettings.load()

    sqs_triggered = 'Records' in event and len(event['Records']) > 0
    if not sqs_triggered:
        print('Lambda not triggered from SQS, aborting.')
        return

    for msg in event['Records']:
        msg_data = json.loads(msg['body'])
        process(msg_data, context, SETTINGS.REGION_NAME)


def process(msg, context, region):
    """
    Process a single message.

    Args:
        msg (dict): Contents described at the top of the file.
        context (Context): Lambda context object.
        region (str): Lambda execution region.
    """

    job_id = int(msg['ingest_job'])
    chunk_key = msg['chunk_key']
    tile_key = msg['tile_key']
    print("Tile key: {}".format(tile_key))

    proj_info = BossIngestProj.fromTileKey(tile_key)

    # Set the job id
    proj_info.job_id = msg['ingest_job']

    print("Data: {}".format(msg))

    # update value in the dynamo table
    tile_index_db = BossTileIndexDB(proj_info.project_name)
    chunk = tile_index_db.getCuboid(chunk_key, job_id)
    if chunk:
        if tile_index_db.cuboidReady(chunk_key, chunk["tile_uploaded_map"]):
            print("Chunk already has all its tiles: {}".format(chunk_key))
            # Go ahead and setup to fire another ingest lambda so this tile
            # entry will be deleted on successful execution of the ingest lambda.
            chunk_ready = True
        else:
            print("Updating tile index for chunk_key: {}".format(chunk_key))
            chunk_ready = tile_index_db.markTileAsUploaded(chunk_key, tile_key, job_id)
    else:
        # First tile in the chunk
        print("Creating first entry for chunk_key: {}".format(chunk_key))
        try:
            tile_index_db.createCuboidEntry(chunk_key, job_id)
        except ClientError as err:
            # Under _exceptional_ circumstances, it's possible for another lambda
            # to beat the current instance to creating the initial cuboid entry
            # in the index.
            error_code = err.response['Error'].get('Code', 'Unknown')
            if error_code == 'ConditionalCheckFailedException':
                print('Chunk key entry already created - proceeding.')
            else:
                raise
        chunk_ready = tile_index_db.markTileAsUploaded(chunk_key, tile_key, job_id)

    # ingest the chunk if we have all the tiles
    if chunk_ready:
        print("CHUNK READY SENDING MESSAGE: {}".format(chunk_key))
        # insert a new job in the insert queue if we have all the tiles
        ingest_queue = IngestQueue(proj_info)
        ingest_queue.sendMessage(json.dumps(msg))

        # Invoke Ingest lambda function
        names = AWSNames.from_lambda(context.function_name)
        lambda_client = boto3.client('lambda', region_name=region)
        lambda_client.invoke(
            FunctionName=names.tile_ingest.lambda_,
            InvocationType='Event',
            Payload=json.dumps(msg).encode())
    else:
        print("Chunk not ready for ingest yet: {}".format(chunk_key))

    print("DONE!")
