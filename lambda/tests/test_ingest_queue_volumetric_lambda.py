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

# lambdafcns contains symbolic links to lambda functions in boss-tools/lambda.
# Since lambda is a reserved word, this allows importing from that folder 
# without updating scripts responsible for deploying the lambda code.

import ingest_queue_upload_volumetric_lambda as iquv
import hashlib
import json
import math
import unittest

class TestIngestQueueUploadLambda(unittest.TestCase):

    def tile_count(self, kwargs):
        x = math.ceil((kwargs["x_stop"] - kwargs["x_start"]) / kwargs["x_tile_size"])
        y = math.ceil((kwargs["y_stop"] - kwargs["y_start"]) / kwargs["y_tile_size"])
        z = math.ceil((kwargs["z_stop"] - kwargs["z_start"]) / kwargs["z_tile_size"])
        t = math.ceil((kwargs["t_stop"] - kwargs["t_start"]) / kwargs["t_tile_size"])
        return x * y * z * t


    def test_all_messages_are_there(self):
        """
        This test will show that when tiles are being genereated by multiple lambdas, the sum of those tiles
        will be the exact same set that would be generated by a single lambda creating them all.
        Test_all_messages_are_there tests() first it creates
        the set of expected messages for the ingest.
        Then it runs the create_messages() _n_ times each time with the appropriate items_to_skip and
        MAX_NUM_ITEMS_PER_LAMBDA set. It pulls the tile key out of the set to verify that all the tiles were
        accounted for.  In the end there should be no tiles left in the set.
        This test can with many different values for tile sizes and starts and stop vaules and num_lambdas can be
        changed.
        """

        # Loops to exercise different parameters.
        for t_stop in [1, 3]:
            for z_stop in [20, 33]:
                for y_stop in [2560, 2048, 2052]:
                    for x_stop in [2048, 2560, 1028]:
                        args = {
                            "upload_sfn": "IngestUpload",
                            "x_start": 0,
                            "x_stop": x_stop,
                            "y_start": 0,
                            "y_stop": y_stop,
                            "z_start": 0,
                            "z_stop": z_stop,
                            "t_start": 0,
                            "t_stop": t_stop,
                            "project_info": [
                              "3",
                              "3",
                              "3"
                            ],
                            "ingest_queue": "https://queue.amazonaws.com/...",
                            "job_id": 11,
                            "upload_queue": "https://queue.amazonaws.com/...",
                            "x_tile_size": 1450,
                            "y_tile_size": 2000,
                            "t_tile_size": 1,
                            "z_tile_size": 3457,
                            "resolution": 0,
                            "items_to_skip": 0,
                            'MAX_NUM_ITEMS_PER_LAMBDA': 500000,
                            'z_chunk_size': 64
                        }

                        # Walk create_messages generator as a single lambda would
                        # and populate the dictionary with all Keys like this
                        # "Chunk --- tiles".
                        msg_set = set()
                        exp_msgs = create_expected_messages(args)
                        for msg_json in exp_msgs:
                            ct_key = generate_chunk_cuboids_key(msg_json)
                            print(ct_key)
                            # if ct_key not in msg_set:
                            #     msg_set.add(ct_key)
                            # else:
                            #     print("Here")
                                #self.fail("Set already contains key: ".format(ct_key))



def generate_chunk_cuboids_key(msg_json):
        """
        Generate a key to track messages for testing.
        Args:
            msg_json (str): JSON message encoded as string intended for the upload queue.
        Returns:
            (str): Unique key identifying message.
        """
        msg = json.loads(msg_json)
        parts = msg["chunk_key"].split("&", 6)
        chunk = parts[-1].replace("&", "  ")
        print(chunk)
        #parts = msg["cuboids"][]["key"].split("&", 5) 
        parts = msg["cuboids"]
        cuboidLength = len(parts)
        for i in range(cuboidLength):
            print(parts[i])
        
        #tile = parts[-1].replace("&", "  ")
        #print("{} --- {}".format(chunk, tile))
        #return "{} --- {}".format(chunk, tile)

def create_expected_messages(args):
        CUBOID_X = 512
        CUBOID_Y = 512
        CUBOID_Z = 16
        tile_size = lambda v: args[v + "_tile_size"]
        # range_ does not work with z. Need to use z_chunk_size instead with volumetric ingest
        range_ = lambda v: range(args[v + '_start'], args[v + '_stop'], tile_size(v))

        # DP NOTE: generic version of
        # BossBackend.encode_chunk_key and BiossBackend.encode.tile_key
        # from ingest-client/ingestclient/core/backend.py
        def hashed_key(*args):
            base = '&'.join(map(str,args))

            md5 = hashlib.md5()
            md5.update(base.encode())
            digest = md5.hexdigest()

            return '&'.join([digest, base])

        chunks_to_skip = args['items_to_skip']
        count_in_offset = 0
        for t in range_('t'):
            for z in range(args['z_start'], args['z_stop'], args['z_chunk_size']):
                for y in range_('y'):
                    for x in range_('x'):

                        if chunks_to_skip > 0:
                            chunks_to_skip -= 1
                            continue

                        if count_in_offset == 0:
                            print("Finished skipping chunks")

                        chunk_x = int(x / tile_size('x'))
                        chunk_y = int(y / tile_size('y'))
                        chunk_z = int(z / args['z_chunk_size'])
                        chunk_key = hashed_key(1,  # num of items
                                            args['project_info'][0],
                                            args['project_info'][1],
                                            args['project_info'][2],
                                            args['resolution'],
                                            chunk_x,
                                            chunk_y,
                                            chunk_z,
                                            t)

                        count_in_offset += 1
                        if count_in_offset > args['MAX_NUM_ITEMS_PER_LAMBDA']:
                            return  # end the generator

                        cuboids = []

                        # Currently, only allow ingest for time sample 0.
                        t = 0
                        lookup_key = iquv.lookup_key_from_chunk_key(chunk_key)
                        res = iquv.resolution_from_chunk_key(chunk_key)

                        for chunk_offset_z in range(0, args["z_chunk_size"], CUBOID_Z):
                            for chunk_offset_y in range(0, tile_size('y'), CUBOID_Y):
                                for chunk_offset_x in range(0, tile_size('x'), CUBOID_X):
                                    morton = '999999'
                                    # morton = XYZMorton(
                                    #     [(x+chunk_offset_x)/CUBOID_X, (y+chunk_offset_y)/CUBOID_Y, (z+chunk_offset_z)/CUBOID_Z])
                                    object_key = iquv.generate_object_key(lookup_key, res, t, morton)
                                    new_cuboid = {
                                        "x": chunk_offset_x,
                                        "y": chunk_offset_y,
                                        "z": chunk_offset_z,
                                        "key": object_key
                                    }
                                    cuboids.append(new_cuboid)

                        msg = {
                            'chunk_key': chunk_key,
                            'cuboids': cuboids,
                        }
                        #print(msg)
                        yield json.dumps(msg)

if __name__ == '__main__':
    unittest.main()