# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

import boto3
import json
import pymysql
import pymysql.cursors
import time
from boss_db import get_db_connection
from bossutils import logger
from ndingest.nddynamo.boss_tileindexdb import TASK_INDEX, MAX_TASK_ID_SUFFIX, TILE_UPLOADED_MAP_KEY
from ingestclient.core.backend import BossBackend

"""
Scans the tile index in DynamoDB for any chunks that have missing tiles.  If
tiles are missing, they are placed back in the upload queue for that ingest
job.  If there are missing tiles, the ingest job's state is reset to UPLOADING.
"""

log = logger.bossLogger()

# Tile index attributes defined in ndingest.git/nddynamo/schemas/boss_tile_index.json.
APPENDED_TASK_ID = 'appended_task_id'
CHUNK_KEY = 'chunk_key'

SQS_BATCH_SIZE = 10
SQS_RETRY_TIMEOUT = 15

# These values are defined in boss.git/django/bossingest/models.py.
UPLOADING_STATUS = 1
TILE_INGEST = 0


def activity_entry_point(args):
    """
    Entry point to the chunk scanner step function activity.

    Args:
        args (dict):
            tile_index_table (str): Name of tile index table.
            region (str): AWS region to use.
            db_host (str): Host of MySQL database.
            job (dict):
                collection (int): Collection id.
                experiment (int): Experiment id.
                channel (int): Channel id.
                task_id (int): The ingest job's id.
                resolution (int): Resolution of chunk.
                z_chunk_size (int): How many z slices in the chunk.
                upload_queue (str): Tile upload queue.
                ingest_queue (str): Tile ingest queue.
                ingest_type (int): Tile (0) or volumetric ingest (1).
            resource (dict): Boss resource data.
            x_size (int): Tile size in x dimension.
            y_size (int): Tile size in y dimension.
            KVIO_SETTINGS: spdb settings.
            STATEIO_CONFIG: spdb settings.
            OBJECTIO_CONFIG: spdb settings.

    Returns:
        (dict): Returns incoming args so they can be passed to the next activity.
                Also adds 'quit' key.  Sets 'quit' to True if missing tiles
                were found.  Otherwise, sets 'quit' to False.
    """

    # This should only run on tile ingests.
    if args['job']['ingest_type'] != TILE_INGEST:
        args['quit'] = False
        return args

    dynamo = boto3.client('dynamodb', region_name=args['region'])
    sqs = boto3.resource('sqs', region_name=args['region'])
    cs = ChunkScanner(dynamo, sqs, args['tile_index_table'], args['db_host'],
                      args['job'], args['resource'], args['x_size'], args['y_size'],
                      args['KVIO_SETTINGS'], args['STATEIO_CONFIG'], args['OBJECTIO_CONFIG'])
    args['quit'] = cs.run()

    return args


class ChunkScanner:
    JOB_FIELDS = frozenset([
        'collection', 'experiment', 'channel',
        'task_id', 'resolution', 'z_chunk_size',
        'upload_queue', 'ingest_queue'
    ])

    def __init__(self, dynamo, sqs, tile_index_table, db_host, job, resource,
                 x_size, y_size, kvio_settings, stateio_config, objectio_config):
        """
        Args:
            dynamo (boto3.Dynamodb): Dynamo client.
            sqs (boto3.SQS.ServiceResource): SQS client.
            tile_index_table (str): Name of tile index table.
            db_host (str): Host of MySQL database.
            job (dict):
                collection (int): Collection id.
                experiment (int): Experiment id.
                channel (int): Channel id.
                task_id (int): The ingest job's id.
                resolution (int): Resolution of chunk.
                z_chunk_size (int): How many z slices in the chunk.
                upload_queue (str): Tile upload queue.
                ingest_queue (str): Tile ingest queue.
            resource (dict): Boss resource data.
            x_size (int): Tile size in x dimension.
            y_size (int): Tile size in y dimension.
            kvio_settings: spdb settings.
            stateio_config: spdb settings.
            objectio_config: spdb settings.
        """
        self.dynamo = dynamo
        self.sqs = sqs
        self.tile_index_table = tile_index_table
        self.db_host = db_host
        self.job = job
        self.resource = resource
        self.x_size = x_size
        self.y_size = y_size
        self.kvio_settings = kvio_settings
        self.stateio_config = stateio_config
        self.objectio_config = objectio_config

        self.found_missing_tiles = False

        # Validate job parameter.
        for field in ChunkScanner.JOB_FIELDS:
            if field not in job:
                raise KeyError('Job must have {}'.format(field))

    def _get_project_info(self):
        """
        Get the project info required by Backend.encode_tile_key().

        Returns:
            (list[str]): [collection, experiment, channel].
        """
        return [self.job['collection'], self.job['experiment'], self.job['channel']]

    def run(self):
        """
        Scan all DynamoDB partitions for remaining chunks in the tile index.
        Tiles missing from chunks are put back in the tile upload queue.

        Returns:
            (bool): True if missing tiles found.
        """
        for i in range(0, MAX_TASK_ID_SUFFIX):
            self.run_scan(i)

        return self.found_missing_tiles

    def run_scan(self, partition_num):
        """
        Scan a single partition for remaining chunks.

        During an ingest, chunks are written across (0, INGEST_MAX_SIZE)
        partitions so Dynamo doesn't throttle the ingest due to a hot partition.
        
        If any remaining chunks are missing tiles, it puts those tiles on the
        upload queue.  After each batch of messages enqueued, it sets the
        state of the ingest job to UPLOADING.  This is done after each batch
        in case the ingest client clears the upload queue and tries to restart
        the complete process.

        self.found_missing_tiles set to True if missing tiles found.

        Args:
            dynamo (boto3.Dynamodb): Dynamo client.
            partition_num (int): Which partition to scan (Suffix appended to task/job id).
        """
        appended_task_id = {'S': '{}_{}'.format(self.job['task_id'], partition_num) }
        query_args = {
            'TableName': self.tile_index_table,
            'IndexName': TASK_INDEX,
            'KeyConditionExpression': '#appended_task_id = :appended_task_id',
            'ExpressionAttributeNames': {
                '#appended_task_id': APPENDED_TASK_ID,
                '#chunk_key': CHUNK_KEY,
                '#tile_uploaded_map': TILE_UPLOADED_MAP_KEY
            },
            'ExpressionAttributeValues': { ':appended_task_id': appended_task_id },
            'ProjectionExpression': '#chunk_key, #tile_uploaded_map'
        }

        db_connection = get_db_connection(self.db_host)

        try:
            upload_queue = self.sqs.Queue(self.job['upload_queue'])
            ingest_queue = self.sqs.Queue(self.job['ingest_queue'])

            query = self.dynamo.get_paginator('query')
            resp_iter = query.paginate(**query_args)
            for resp in resp_iter:
                for item in resp['Items']:
                    missing_msgs = self.check_tiles(item[CHUNK_KEY]['S'], item[TILE_UPLOADED_MAP_KEY]['M'])
                    no_missing_tiles = False
                    if self.enqueue_missing_tiles(upload_queue, missing_msgs):
                        self.found_missing_tiles = True
                        no_missing_tiles = True
                        self.set_uploading_status(db_connection)
                    if no_missing_tiles:
                        self.enqueue_chunk(ingest_queue, item[CHUNK_KEY]['S'])
        finally:
            db_connection.close()

    def enqueue_chunk(self, queue, chunk_key):
        """
        Put the chunk back in the ingest queue.  All its tiles should be in S3,
        but the ingest lambda must have failed.

        Args:
            queue (sqs.Queue): Ingest queue.
            chunk_key (str): Key identifying which chunk to re-ingest.
        """
        raw_msg = {
            'chunk_key': chunk_key,
            'ingest_job': self.job,
            'parameters': {
                'KVIO_SETTINGS': self.kvio_settings,
                'STATEIO_CONFIG': self.stateio_config,
                'OBJECTIO_CONFIG': self.objectio_config,
                'resource': self.resource,
            },
            'x_size': self.tile_size_x,
            'y_size': self.tile_size_y,
        }
        queue.send_message(MessageBody=json.dumps(raw_msg))


    def set_uploading_status(self, db_connection):
        """
        Set the status of the ingest job to UPLOADING.

        Args:
            db_connection (pymysql.Connection)
        """
        sql = 'UPDATE ingest_job SET status = %(status)s WHERE id = %(job_id)s'
        sql_args = dict(status=str(UPLOADING_STATUS), job_id=str(self.job['task_id']))
        try:
            with db_connection.cursor(pymysql.cursors.SSCursor) as cursor:
                rows = cursor.execute(sql, sql_args)
                if rows < 1:
                    log.error(
                        'DB said no rows updated when trying to set UPLOADING job status for job: {}'.format(
                            self.job['task_id'])
                    )
        except Exception as ex:
            log.error('Failed to set UPLOADING status: {}'.format(ex))

    def check_tiles(self, chunk_key, tiles):
        """
        Check the chunk's tile map for missing tiles.  If any are missing,
        generate the proper stringified JSON for putting those missing tiles
        back in the tile upload queue.

        Args:
            chunk_key (str): Identifies chunk of tiles.
            tiles (): List of tiles uploaded for the chunk.
        Yields:
            (str): JSON string for sending to SQS tile upload queue.
        """
        # Only using encode|decode_*_key methods, so don't need to provide a
        # config.
        ingest_backend = BossBackend(None)
        chunk_key_parts = ingest_backend.decode_chunk_key(chunk_key)
        chunk_x = chunk_key_parts['x_index']
        chunk_y = chunk_key_parts['y_index']
        chunk_z = chunk_key_parts['z_index']
        t = chunk_key_parts['t_index']
        num_tiles = chunk_key_parts['num_tiles']
        z_start = chunk_z * self.job['z_chunk_size']

        for tile_z in range(z_start, z_start + num_tiles):
            # First arg is a list of [collection, experiment, channel] ids.
            tile_key = ingest_backend.encode_tile_key(
                self._get_project_info(), self.job['resolution'], chunk_x, chunk_y, tile_z, t)
            if tile_key in tiles:
                continue
            msg = {
                'job_id': self.job['task_id'],
                'upload_queue_arn': self.job['upload_queue'],
                'ingest_queue_arn': self.job['ingest_queue'],
                'chunk_key': chunk_key,
                'tile_key': tile_key
            }

            yield json.dumps(msg)

    def enqueue_missing_tiles(self, queue, msgs):
        """
        Send messages for missing tiles to the upload queue.

        Args:
            queue (SQS.Queue): The upload queue.
            msgs (Iterator[str]): Stringified JSON messages. 

        Returns:
            (bool): True if at least one message was enqueued.
        """
        enqueued_msgs = False

        while True:
            batch = []
            for i in range(SQS_BATCH_SIZE):
                try:
                    batch.append({
                        'Id': str(i),
                        'MessageBody': next(msgs),
                        'DelaySeconds': 0
                    })
                except StopIteration:
                    break

            if len(batch) == 0:
                break

            retry = 3
            while retry > 0:
                resp = queue.send_messages(Entries=batch)
                if 'Failed' in resp and len(resp['Failed']) > 0:
                    time.sleep(SQS_RETRY_TIMEOUT)

                    ids = [f['Id'] for f in resp['Failed']]
                    batch = [b for b in batch if b['Id'] in ids]
                    retry -= 1
                    if retry == 0:
                        log.error('Could not send {}/{} messages to queue {}'.format(
                            len(resp['Failed']), len(batch), queue.url))
                        break
                else:
                    enqueued_msgs = True
                    break

        return enqueued_msgs
