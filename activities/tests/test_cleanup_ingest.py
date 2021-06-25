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


import pymysql
import pymysql.cursors
import unittest
from unittest.mock import MagicMock, patch
from activities.cleanup_ingest import IngestCleaner, UPLOADING_DB, TILE_INGEST, VOLUMETRIC_INGEST
from datetime import datetime

END_TIME = datetime(2020, 1, 1, 13, 0)

class TestIngestCleaner(unittest.TestCase):

    def make_mock(self, name):
        """
        Create a MagicMock for the given item.

        Args:
            name (str): Name of item to mock.

        Returns:
            (MagicMock)
        """
        patch_wrapper = patch(name, autospec=True)
        magic_mock = patch_wrapper.start()
        # This ensures the patch is removed when the test is torn down.
        self.addCleanup(patch_wrapper.stop)
        return magic_mock

    def test_delete_lambda_event_source_queues_does_nothing_for_volumetric_ingests(self):
        status = 'complete'
        db_host = 'bar'
        job_id = 300
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'ingest_type': VOLUMETRIC_INGEST
        }

        cleaner = IngestCleaner(status, db_host, job)
        with patch.object(cleaner, 'remove_sqs_event_source_from_lambda', autospec=True) as remove_spy:
            cleaner.delete_lambda_event_source_queues()
            self.assertEqual(0, len(remove_spy.call_args_list))

    def test_delete_lambda_event_source_queues(self):
        status = 'complete'
        db_host = 'bar'
        job_id = 300
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'ingest_type': TILE_INGEST
        }

        fake_ingest_q = self.make_mock('activities.cleanup_ingest.IngestQueue')
        fake_ingest_q.arn.return_value = 'ingest_queue_arn'
        fake_tile_index_q = self.make_mock('activities.cleanup_ingest.TileIndexQueue')
        fake_tile_index_q.arn.return_value = 'tile_ingest_queue_arn'

        boss_cfg = self.make_mock('activities.cleanup_ingest.BossConfig')
        boss_cfg.return_value = { 'aws': {
            'tile_ingest_lambda': 'tileIngestLambda',
            'tile_uploaded_lambda': 'tileUploadedLambda',
        } }

        cleaner = IngestCleaner(status, db_host, job)
        with patch.object(cleaner, 'remove_sqs_event_source_from_lambda', autospec=True) as remove_spy:
            cleaner.delete_lambda_event_source_queues()
            # Should be called twice to remove two queues.
            self.assertEqual(2, len(remove_spy.call_args_list))

    def test_delete_queues_non_tile_ingest(self):
        """Should only delete upload queue for non-tile ingests."""

        status = 'complete'
        db_host = 'bar'
        job_id = 300
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'ingest_type': VOLUMETRIC_INGEST
        }

        fake_upload_q = self.make_mock('activities.cleanup_ingest.UploadQueue')
        fake_ingest_q = self.make_mock('activities.cleanup_ingest.IngestQueue')
        fake_tile_index_q = self.make_mock('activities.cleanup_ingest.TileIndexQueue')
        fake_tile_error_q = self.make_mock('activities.cleanup_ingest.TileErrorQueue')

        cleaner = IngestCleaner(status, db_host, job)
        cleaner.delete_queues()

        fake_upload_q.deleteQueue.assert_called_once()
        fake_ingest_q.deleteQueue.assert_not_called()
        fake_tile_index_q.deleteQueue.assert_not_called()
        fake_tile_error_q.deleteQueue.assert_not_called()

    def test_delete_queues_tile_ingest(self):
        """Should only delete 4 different queues for tile ingests."""

        status = 'complete'
        db_host = 'bar'
        job_id = 300
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'ingest_type': TILE_INGEST
        }

        fake_upload_q = self.make_mock('activities.cleanup_ingest.UploadQueue')
        fake_ingest_q = self.make_mock('activities.cleanup_ingest.IngestQueue')
        fake_tile_index_q = self.make_mock('activities.cleanup_ingest.TileIndexQueue')
        fake_tile_error_q = self.make_mock('activities.cleanup_ingest.TileErrorQueue')

        cleaner = IngestCleaner(status, db_host, job)
        cleaner.delete_queues()

        fake_upload_q.deleteQueue.assert_called_once()
        fake_ingest_q.deleteQueue.assert_called_once()
        fake_tile_index_q.deleteQueue.assert_called_once()
        fake_tile_error_q.deleteQueue.assert_called_once()

    @unittest.skip('Manual test requiring the test_microns DB.')
    @patch('activities.cleanup_ingest.datetime')
    def test_set_status(self, fake_datetime):
        """
        This test is not meant to be run as part of CI.  It tests that
        IngestCleaner.set_status() works, but requires the test database
        created by Django.
        `boss.git/django/manage.py testserver --settings=boss.settings.mysql empty_fixture.json`.

        empty_fixture.json merely contains {} because we do all necessary setup
        within this test.
        """

        fake_datetime.now.return_value = END_TIME
        user_id = 9999
        db_host = 'bar'
        job_id = 1000
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'ingest_type': TILE_INGEST
        }

        user_insert = """
            INSERT INTO auth_user (
                id, password, is_superuser, username, first_name, last_name, email,
                is_staff, is_active, date_joined
            )
            VALUES (
                %(id)s, %(password)s, %(is_superuser)s, %(username)s,
                %(first_name)s, %(last_name)s, %(email)s,
                %(is_staff)s, %(is_active)s, %(date_joined)s
            );
        """

        user_insert_args = dict(
            id=str(user_id), password='foo', is_superuser='1', username='admin',
            first_name='', last_name='', email='',
            is_staff='1', is_active='1', date_joined='2020-01-01 00:00:00'
        )

        insert = """
            INSERT INTO ingest_job (
                id, status, ingest_type, start_date, config_data,
                collection, experiment, channel, resolution,
                collection_id, experiment_id, channel_id,
                creator_id, tile_count,
                step_function_arn, wait_on_queues_ts,
                x_start, y_start, z_start, t_start,
                x_stop, y_stop, z_stop, t_stop,
                tile_size_x, tile_size_y, tile_size_z, tile_size_t
            )
            VALUES (
                %(id)s, %(status)s, %(ingest_type)s, %(start_date)s,
                %(config_data)s,
                %(collection)s, %(experiment)s, %(channel)s, %(resolution)s,
                %(collection_id)s, %(experiment_id)s, %(channel_id)s,
                %(creator_id)s, %(tile_count)s,
                %(step_function_arn)s, %(wait_on_queues_ts)s,
                %(x_start)s, %(y_start)s, %(z_start)s, %(t_start)s,
                %(x_stop)s, %(y_stop)s, %(z_stop)s, %(t_stop)s,
                %(tile_size_x)s, %(tile_size_y)s, %(tile_size_z)s, %(tile_size_t)s
            );
            """

        insert_args = dict(
            id=str(job_id), status=str(UPLOADING_DB), ingest_type=str(TILE_INGEST),
            start_date='2020-01-01 00:00:00', config_data='',
            collection='', experiment='', channel='', resolution='0',
            collection_id=None, experiment_id=None, channel_id=None,
            creator_id=str(user_id), tile_count='100',
            step_function_arn='', wait_on_queues_ts=None,
            x_start='0', y_start='0', z_start='0', t_start='0',
            x_stop='10', y_stop='10', z_stop='10', t_stop='1',
            tile_size_x='1024', tile_size_y='1024', tile_size_z='32', tile_size_t='1' 
        )

        select = 'SELECT status, ingest_queue, upload_queue, end_date FROM ingest_job WHERE id = %(id)s;'
        select_args = dict(id=str(job_id))

        db_connection = pymysql.connect(
            host='localhost',
            user='root',
            password='MICrONS',
            db='test_microns',
            port=3306,
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)

        try:
            with db_connection.cursor() as cursor:
                for status in IngestCleaner.STATUS_VALUES:
                    with self.subTest(status):
                        try:
                            user_rows = cursor.execute(user_insert, user_insert_args)
                            self.assertEqual(1, user_rows)
                            rows = cursor.execute(insert, insert_args)
                            self.assertEqual(1, rows)

                            cleaner = IngestCleaner(status, db_host, job)
                            cleaner.set_status(db_connection)

                            found_rows = cursor.execute(select, select_args)
                            self.assertEqual(1, found_rows)
                            job_row = cursor.fetchone()
                            self.assertEqual(cleaner._status_map[status], job_row['status'])
                            self.assertIsNone(job_row['ingest_queue'])
                            self.assertIsNone(job_row['upload_queue'])
                            self.assertEqual(END_TIME, job_row['end_date'])
                        finally:
                            # Clean up so test can be run again w/o failing.
                            try:
                                cursor.execute('DELETE FROM ingest_job WHERE id = %(id)s', dict(id=str(job_id)))
                            except Exception:
                                pass
                            try:
                                cursor.execute('DELETE FROM auth_user WHERE id = %(id)s', dict(id=str(user_id)))
                            except Exception:
                                pass
        finally:
            db_connection.close()
