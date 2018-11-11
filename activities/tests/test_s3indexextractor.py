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

from activities.delete_cuboid import S3IndexExtractor, ResJobId
import pymysql
from spdb.spatialdb.object import AWSObjectStore
import unittest
from unittest.mock import patch, MagicMock, DEFAULT

class S3IndexExtractorTest(unittest.TestCase):
    def setUp(self):
        self.session = MagicMock()

    @patch('activities.delete_cuboid.put_json_in_s3', autospec=True)
    @patch.object(S3IndexExtractor, '_get_resolutions_and_job_ids', autospec=True)
    @patch.object(S3IndexExtractor, '_query_ingest_id_index', autospec=True)
    def test_writing_to_shards(self, fake_query_ind, fake_get_res_job_ids, fake_put_json):

        coll_id = 1
        exp_id = 4
        chan_id = 2
        data = {
            'delete_bucket': 'delbuck',
            's3-index-table': 's3.example.com',
            'lookup_key': '{}&{}&{}'.format(coll_id, exp_id, chan_id)
        }
        res = 0
        job_id = 21
        num_hierarchy_levels = 4

        fake_get_res_job_ids.return_value = ([ResJobId(res=i, job_id=job_id) 
            for i in range(res, num_hierarchy_levels)])

        def mock_query_ind(self, key):
            """Only return keys if it's the ingest job."""
            desired_key = AWSObjectStore.get_ingest_id_hash(
                coll_id, exp_id, chan_id, res, job_id, 0) 
            if desired_key != key:
                return []

            return [{'Items': [
                    { 'object-key': 'a1', 'version-node': 0 },
                    { 'object-key': 'a2', 'version-node': 0 },
                    { 'object-key': 'a3', 'version-node': 0 }
                ]}]

        fake_query_ind.side_effect = mock_query_ind

        max_id_suffix = 0
        max_items_per_shard = 2

        s3_ind_ext = S3IndexExtractor(data, self.session)
        actual = s3_ind_ext.start([], max_id_suffix, max_items_per_shard)

        self.assertIn('delete_shard_index_key', actual)

        # Expect queries for each resolution for the one job id and one for
        # potential cuboids added via cutouts at native resolution.
        self.assertEqual(1+num_hierarchy_levels, fake_query_ind.call_count)

        # Should be called twice to write 2 shards and once to write master
        # index key.
        self.assertEqual(3, fake_put_json.call_count)

    @patch('activities.delete_cuboid.get_db_connection', autospec=True)
    def test_get_resolutions_and_job_ids(self, fake_get_db_conn):
        """
        Test that in addition to the resolution set in the ingest_job table,
        additional resolutions up to MAX_RESOLUTIONS also returned in case
        the channel was downsampled.
        """
        coll_id = 1
        exp_id = 4
        chan_id = 2
        data = {
            'delete_bucket': 'delbuck',
            's3-index-table': 's3.example.com',
            'lookup_key': '{}&{}&{}'.format(coll_id, exp_id, chan_id)
        }
        res = 0
        job_id = 21
        num_hierarchy_levels = 4

        fake_cursor = MagicMock(spec=pymysql.cursors.SSCursor)
        fake_cursor.fetchall_unbuffered.return_value = [(job_id, res)]
        fake_cursor.fetchone.return_value = [num_hierarchy_levels]
        fake_cursor.execute.return_value = 1
        # Make this work when used with a context manager.
        fake_cursor.__enter__.return_value = fake_cursor
        fake_conn = MagicMock(spec=pymysql.connections.Connection)
        fake_conn.cursor.return_value = fake_cursor
        fake_get_db_conn.return_value = fake_conn

        s3_ind_ext = S3IndexExtractor(data, self.session)
        actual = s3_ind_ext._get_resolutions_and_job_ids(coll_id, exp_id, chan_id)

        expected = ([ResJobId(res=i, job_id=job_id) 
            for i in range(res, num_hierarchy_levels)])
        self.assertEqual(expected, actual)
