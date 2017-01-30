# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Need:
# * ingest-client
# * ndingest
from ingest.core.config import Configuration
from ingest.core.backend import BossBackend

from ndingest.ndingestproj.bossingestproj import BossIngestProj

SQS_BATCH_SIZE = 10
def populate_upload_queue(args):
    """
    args: {
        'job_id': '',
        'upload_queue': ARN,
        'ingest_queue': ARN,

        'collection_name': '',
        'experiment_name': '',
        'channel_name': '',

        'resolution': 0,
        'project_info': [col_id, exp_id, ch_id],
        
        't_start': 0,
        't_stop': 0,
        't_tile_size': 0,

        'x_start': 0,
        'x_stop': 0,
        'x_tile_size': 0,

        'y_start': 0,
        'y_stop': 0
        'y_tile_size': 0,

        'z_start': 0,
        'z_stop': 0
        'z_tile_size': 16,
    }
    """

    msgs = create_messages(args)
    batches = (msgs[x:x+SQS_BATCH_SIZE] for x in range(0, len(msgs), SQS_PATCH_SIZE))

    proj = BossIngestProj(args['collection_name'],
                          args['experiment_name'],
                          args['channel_name'],
                          args['resolution'],
                          args['job_id'])
    queue = UploadQueue(proj)

    for batch in batches:
        resp = queue.sendBatchMessages(batch)
        # { 'Successful': [], 'Failed': [] }

    return len(msgs)

def create_messages(args):
    tile_size = lambda v: args[v + "_tile_size"]
    range_ = lambda v: range(args[v + '_start'], args[v + '_stop'], tile_size(v))

    # DP NOTE: configuration is not actually used by encode_*_key method
    backend = BossBackend(None)

    msgs = []
    for t in range_('t'):
        for z in range_('z'):
            for y in range_('y'):
                for x in range_('x'):
                    chunk_x = int(x/tile_size('x'))
                    chunk_y = int(y/tile_size('y'))
                    chunk_z = int(z/tile_size('z'))

                    num_of_tiles = min(tile_size('z'), args['z_stop'] - z)

                    chuck_key = backend.encode_chunk_key(num_of_tiles,
                                                         args['project_info'],
                                                         args['resolution'],
                                                         chunk_x,
                                                         chunk_y,
                                                         chunk_z,
                                                         t)

                    for tile in range(z, z + num_of_tiles):
                        tile_key = backend.encode_tile_key(args['project_info'],
                                                           args['resolution'],
                                                           chunk_x,
                                                           chunk_y,
                                                           tile,
                                                           t)

                        msg = {
                            'job_id': args['job_id'],
                            'upload_queue_arn': args['upload_queue'],
                            'ingest_queue_arn': args['ingest_queue'],
                            'chunk_key': chunk_key,
                            'tile_key': tile_key,
                        }

                        msgs.append(json.dumps(msg))

    return msgs

