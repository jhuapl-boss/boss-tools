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


import json
import unittest
from unittest.mock import call, patch, MagicMock
from ingestclient.core.backend import BossBackend
from activities.scan_for_missing_chunks import ChunkScanner, SQS_BATCH_SIZE, UPLOADING_STATUS, TILE_INGEST
import pymysql
import pymysql.cursors

class TestChunkScanner(unittest.TestCase):
    def test_check_tiles(self):
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        chunk_x = 94
        chunk_y = 40
        chunk_z = int(2128 / job['z_chunk_size'])

        # Simulate the tile map in the Dynamo tile index.
        tile_map = {
            '51b03272fb38672209b1891a5b6e20b8&90&141&985&0&94&40&2128&0': {'N': '1'},
            '1743c07e50c1a642e357d6738cab13e9&90&141&985&0&94&40&2129&0': {'N': '1'},
            'de1a408932bcfc3d36b5b4a7607d0964&90&141&985&0&94&40&2130&0': {'N': '1'},
            '12223001b0bc18b3a8cb3463b09552d8&90&141&985&0&94&40&2131&0': {'N': '1'},
            '82ae0d1f7775f29e30491036fd0335e2&90&141&985&0&94&40&2132&0': {'N': '1'},
            '4b0c2f87566cbcdffb9b89ee873c0949&90&141&985&0&94&40&2134&0': {'N': '1'},
            '6f13405cda1b04907b9ed3f9795eb9fb&90&141&985&0&94&40&2135&0': {'N': '1'},
            'ab42d5b88996f9a0076c51f13f082173&90&141&985&0&94&40&2136&0': {'N': '1'},
            'fb3bdfff8982a8250a9f21c30098d2e5&90&141&985&0&94&40&2137&0': {'N': '1'},
            '2853c92ea467cf0e3f571bba11a63473&90&141&985&0&94&40&2138&0': {'N': '1'},
            '6700434c27e4fb23de45fe0df9f2162f&90&141&985&0&94&40&2139&0': {'N': '1'},
            '2fb7f8223dae87bf8d9c6bd73f7e3d2a&90&141&985&0&94&40&2141&0': {'N': '1'},
            '6483f9efb8da500d010e1ea70b956ebe&90&141&985&0&94&40&2142&0': {'N': '1'},
            'b8f59755d83f8501e387fb15329ee7ee&90&141&985&0&94&40&2143&0': {'N': '1'},
        }

        # The tile map is missing tiles with z=2133 and 2140.
        missing_tiles = [
            '16a888bbb0457cb8e6bfce882b74afff&90&141&985&0&94&40&2133&0',
            '4a1c5be8336960d75fb4c4366544a9d3&90&141&985&0&94&40&2140&0'
        ]

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        backend = BossBackend(None)
        chunk_key = backend.encode_chunk_key(16, cs._get_project_info(), job['resolution'],
            chunk_x, chunk_y, chunk_z)

        msg_template = {
            'job_id': job['task_id'],
            'upload_queue_arn': 'foo',
            'ingest_queue_arn': 'bar',
            'chunk_key': chunk_key
        }

        # Expect messages for tile upload queue for tiles with z=2133 and 2140.
        exp = [json.dumps(dict(msg_template, tile_key=tile)) for tile in missing_tiles]
        actual = [tile for tile in cs.check_tiles(chunk_key, tile_map)]
        self.assertCountEqual(exp, actual)

    def test_check_tiles_no_missing_tiles(self):
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        chunk_x = 94
        chunk_y = 40
        chunk_z = int(2128 / job['z_chunk_size'])

        # Simulate the tile map in the Dynamo tile index.
        tile_map = {
            '51b03272fb38672209b1891a5b6e20b8&90&141&985&0&94&40&2128&0': {'N': '1'},
            '1743c07e50c1a642e357d6738cab13e9&90&141&985&0&94&40&2129&0': {'N': '1'},
            'de1a408932bcfc3d36b5b4a7607d0964&90&141&985&0&94&40&2130&0': {'N': '1'},
            '12223001b0bc18b3a8cb3463b09552d8&90&141&985&0&94&40&2131&0': {'N': '1'},
            '82ae0d1f7775f29e30491036fd0335e2&90&141&985&0&94&40&2132&0': {'N': '1'},
            '16a888bbb0457cb8e6bfce882b74afff&90&141&985&0&94&40&2133&0': {'N': '1'},
            '4b0c2f87566cbcdffb9b89ee873c0949&90&141&985&0&94&40&2134&0': {'N': '1'},
            '6f13405cda1b04907b9ed3f9795eb9fb&90&141&985&0&94&40&2135&0': {'N': '1'},
            'ab42d5b88996f9a0076c51f13f082173&90&141&985&0&94&40&2136&0': {'N': '1'},
            'fb3bdfff8982a8250a9f21c30098d2e5&90&141&985&0&94&40&2137&0': {'N': '1'},
            '2853c92ea467cf0e3f571bba11a63473&90&141&985&0&94&40&2138&0': {'N': '1'},
            '6700434c27e4fb23de45fe0df9f2162f&90&141&985&0&94&40&2139&0': {'N': '1'},
            '4a1c5be8336960d75fb4c4366544a9d3&90&141&985&0&94&40&2140&0': {'N': '1'},
            '2fb7f8223dae87bf8d9c6bd73f7e3d2a&90&141&985&0&94&40&2141&0': {'N': '1'},
            '6483f9efb8da500d010e1ea70b956ebe&90&141&985&0&94&40&2142&0': {'N': '1'},
            'b8f59755d83f8501e387fb15329ee7ee&90&141&985&0&94&40&2143&0': {'N': '1'},
        }

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        backend = BossBackend(None)
        chunk_key = backend.encode_chunk_key(16, cs._get_project_info(), job['resolution'],
            chunk_x, chunk_y, chunk_z)
        actual = cs.check_tiles(chunk_key, tile_map)

        # Test that no tiles are returned.
        with self.assertRaises(StopIteration):
            next(actual)

    def test_enqueue_missing_tiles_no_errors(self):
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        queue = MagicMock()
        queue.send_messages.side_effect = [
            { 'Success': 10 },
            { 'Success': 4 }
        ]

        num_msgs = 15
        self.assertGreater(num_msgs, SQS_BATCH_SIZE)

        msgs = [str(i) for i in range(0, num_msgs)]
        exp_calls = [
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(0, SQS_BATCH_SIZE)]),
            call(Entries=[{'Id': str(i-SQS_BATCH_SIZE), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(SQS_BATCH_SIZE, num_msgs)])
        ]

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        self.assertTrue(cs.enqueue_missing_tiles(queue, iter(msgs)))

        self.assertEqual(exp_calls, queue.send_messages.call_args_list)

    # Replace sleep so test runs fast.
    @patch('time.sleep', autospec=True)
    def test_enqueue_missing_tiles_with_retry(self, fake_sleep):
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        queue = MagicMock()
        queue.send_messages.side_effect = [
            { 'Failed': [{ 'Id': '3' }, { 'Id': '7' }] },
            {},
            {}
        ]

        num_msgs = 15
        self.assertGreater(num_msgs, SQS_BATCH_SIZE)

        msgs = [str(i) for i in range(0, num_msgs)]
        exp_calls = [
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(0, SQS_BATCH_SIZE)]),
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0} for i in [3, 7]]),
            call(Entries=[{'Id': str(i-SQS_BATCH_SIZE), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(SQS_BATCH_SIZE, num_msgs)])
        ]

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        self.assertTrue(cs.enqueue_missing_tiles(queue, iter(msgs)))

        self.assertEqual(exp_calls, queue.send_messages.call_args_list)

    # Replace sleep so test runs fast.
    @patch('time.sleep', autospec=True)
    def test_enqueue_missing_tiles_with_errors(self, fake_sleep):
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        queue = MagicMock()
        queue.send_messages.side_effect = [
            { 'Failed': [{ 'Id': '3' }, { 'Id': '7' }] },
            { 'Failed': [{ 'Id': '3' }, { 'Id': '7' }] },
            { 'Failed': [{ 'Id': '3' }, { 'Id': '7' }] },
            {},
            {}
        ]

        num_msgs = 15
        self.assertGreater(num_msgs, SQS_BATCH_SIZE)

        msgs = [str(i) for i in range(0, num_msgs)]
        exp_calls = [
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(0, SQS_BATCH_SIZE)]),
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0} for i in [3, 7]]),
            call(Entries=[{'Id': str(i), 'MessageBody': msgs[i], 'DelaySeconds': 0} for i in [3, 7]]),
            call(Entries=[{'Id': str(i-SQS_BATCH_SIZE), 'MessageBody': msgs[i], 'DelaySeconds': 0}
                for i in range(SQS_BATCH_SIZE, num_msgs)])
        ]

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        self.assertTrue(cs.enqueue_missing_tiles(queue, iter(msgs)))

        self.assertEqual(exp_calls, queue.send_messages.call_args_list)

    # Replace sleep so test runs fast.
    @patch('time.sleep', autospec=True)
    def test_enqueue_missing_tiles_no_msgs(self, fake_sleep):
        """Should return False and not enqueue any messages if not given any."""
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': 1082,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
        }

        queue = MagicMock()

        msgs = []

        cs = ChunkScanner(dynamo, sqs, table, db_host, job)
        self.assertFalse(cs.enqueue_missing_tiles(queue, iter(msgs)))
        queue.send_messages.assert_not_called()

    @unittest.skip('Manual test requiring the test_microns DB.')
    def test_set_uploading_status(self):
        """
        This test is not meant to be run as part of CI.  It tests that
        ChunkScanner.set_uploading_status() works, but requires the test
        database created by Django.
        `boss.git/django/manage.py testserver --settings=boss.settings.mysql empty_fixture.json`.

        empty_fixture.json merely contains {} because we do all necessary setup
        within this test.
        """
        user_id = 9999
        job_id = 1000
        dynamo = None
        sqs = None
        table = 'foo'
        db_host = 'bar'
        job = {
            'collection': 90,
            'experiment': 141,
            'channel': 985,
            'task_id': job_id,
            'resolution': 0,
            'z_chunk_size': 16,
            'upload_queue': 'foo',
            'ingest_queue': 'bar'
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
            id=str(job_id), status=str(UPLOADING_STATUS+1), ingest_type=str(TILE_INGEST),
            start_date='2020-01-01 00:00:00', config_data='',
            collection='', experiment='', channel='', resolution='0',
            collection_id=None, experiment_id=None, channel_id=None,
            creator_id=str(user_id), tile_count='100',
            step_function_arn='', wait_on_queues_ts=None,
            x_start='0', y_start='0', z_start='0', t_start='0',
            x_stop='10', y_stop='10', z_stop='10', t_stop='1',
            tile_size_x='1024', tile_size_y='1024', tile_size_z='32', tile_size_t='1' 
        )

        select = 'SELECT status FROM ingest_job WHERE id = %(id)s;'
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
                try:
                    user_rows = cursor.execute(user_insert, user_insert_args)
                    self.assertEqual(1, user_rows)
                    rows = cursor.execute(insert, insert_args)
                    self.assertEqual(1, rows)

                    cs = ChunkScanner(dynamo, sqs, table, db_host, job)
                    cs.set_uploading_status(db_connection)

                    found_rows = cursor.execute(select, select_args)
                    self.assertEqual(1, found_rows)
                    job_row = cursor.fetchone()
                    self.assertEqual(UPLOADING_STATUS, job_row['status'])
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
