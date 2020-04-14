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


import pymysql.cursors
from datetime import datetime, timezone
from boss_db import get_db_connection
from bossutils import logger
from bossutils.ingestcreds import IngestCredentials
from bossutils.utils import set_excepthook
from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.ndqueue.tileindexqueue import TileIndexQueue
from ndingest.ndqueue.tileerrorqueue import TileErrorQueue
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.util.bossutil import BossUtil

"""
This activity cleans up the SQS queues and credentials that were created for
an ingest job.

Assumptions:
  If this activity is invoked, then the ingest job's resources should be
  completely removed.  Either the ingest job completed successfully or it was
  deleted.
"""

# Hook up Boss exception handler.
set_excepthook()
log = logger.BossLogger().logger

# Accepted status values to IngestCleaner.
COMPLETE_STATUS = 'complete'
DELETED_STATUS = 'deleted'

# Values from boss.git/django/bossingest/models.py.
UPLOADING_DB = 1
COMPLETE_DB = 2
DELETED_DB = 3
TILE_INGEST = 0
VOLUMETRIC_INGEST = 1

class BadStatusError(Exception):
    """Raised when provided status is not a valid value."""


def activity_entry_point(args):
    """
    Entry point when invoked by a step function.

    Args:
        args (dict):
            status (str): 'deleted' or 'complete'.
            db_host (str): Host of MySQL database.
            job (dict):
                collection (int): Collection id.
                experiment (int): Experiment id.
                channel (int): Channel id.
                task_id (int): The ingest job's id.
                resolution (int): Resolution of chunk.
                ingest_type (int): Tile (0) or volumetric ingest (1).
    """
    cleaner = IngestCleaner(args['status'], args['db_host'], args['job'])
    cleaner.run()


class IngestCleaner:
    STATUS_VALUES = frozenset([COMPLETE_STATUS, DELETED_STATUS])
    JOB_FIELDS = frozenset([
        'collection', 'experiment', 'channel',
        'task_id', 'resolution', 'ingest_type'
    ])

    def __init__(self, status, db_host, job):
        """
        Args:
            status (str): Mark ingest job as 'deleted' or 'complete'.
            db_host (str): Host of MySQL database.
            job (dict):
                collection (int): Collection id.
                experiment (int): Experiment id.
                channel (int): Channel id.
                task_id (int): The ingest job's id.
                resolution (int): Resolution of chunk.
                ingest_type (int): Tile (0) or volumetric ingest (1).
        """
        self._status_map = { COMPLETE_STATUS: COMPLETE_DB, DELETED_STATUS: DELETED_DB }
        self.db_host = db_host
        self.job = job

        # Validate job parameter.
        for field in IngestCleaner.JOB_FIELDS:
            if field not in job:
                raise KeyError('Job must have {}'.format(field))

        # Validate status parameter.
        self.status = status
        if status.lower() not in IngestCleaner.STATUS_VALUES:
            raise BadStatusError('{} is not a valid status.'.format(status))

        proj_class = BossIngestProj.load()

        # coll/exp/chan are specified as strings, but not necessarily to give
        # actual names when using ndingest's delete functionality.
        self.nd_proj = proj_class(job['collection'], job['experiment'], job['channel'],
                                  job['resolution'], job['task_id'])

    def run(self):
        """
        Start the cleanup process.
        """
        self.delete_queues()
        self.delete_credentials()

        db_connection = get_db_connection(self.db_host)
        try:
            self.set_status(db_connection)
        finally:
            db_connection.close()

    def delete_queues(self):
        """
        Delete all the queues used by the ingest job.
        """
        delete_list = [UploadQueue]
        #UploadQueue.deleteQueue(self.nd_proj, endpoint_url=None)
        if int(self.job['ingest_type']) == TILE_INGEST:
            delete_list.append(IngestQueue)
            delete_list.append(TileIndexQueue)
            delete_list.append(TileErrorQueue)
            #IngestQueue.deleteQueue(self.nd_proj, endpoint_url=None)
            #TileIndexQueue.deleteQueue(self.nd_proj, endpoint_url=None, delete_deadletter_queue=True)
            #TileErrorQueue.deleteQueue(self.nd_proj, endpoint_url=None)

        for queue in delete_list:
            try:
                queue.deleteQueue(self.nd_proj, endpoint_url=None)
            except Exception as ex:
                log.warn('Caught exception deleting: {} - {}'.format(queue.url, ex))

    def delete_credentials(self):
        """
        Delete AWS credentials allocated for ingest.
        """
        ingest_creds = IngestCredentials()
        ingest_creds.remove_credentials(self.job['task_id'])
        if not BossUtil.delete_ingest_policy(self.job['task_id']):
            log.error('Failed to delete credentials for job: {}'.format(
                self.job['task_id'])
            )

    def set_status(self, db_connection):
        """
        Set the status for the job in the MySQL database.

        Args:
            db_connection (pymysql.Connection)
        """
        sql = """
            UPDATE ingest_job
            SET status = %(status)s, ingest_queue = %(null)s, upload_queue = %(null)s, end_date = %(timestamp)s
            WHERE id = %(job_id)s
            """

        sql_args = dict(
            status=str(self._status_map[self.status]),
            job_id=str(self.job['task_id']),
            null=None,
            timestamp=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        )

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
