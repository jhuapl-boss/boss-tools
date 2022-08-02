import boto3
import json
import math
import time
import hashlib
import pprint
from spdb.c_lib.ndlib import XYZMorton


class FailedToSendMessages(Exception):
    pass


SQS_BATCH_SIZE = 10
SQS_RETRY_TIMEOUT = 15

# Boss cuboid dimensions.
CUBOID_X = 512
CUBOID_Y = 512
CUBOID_Z = 16


def handler(args, context):
    """Populate the ingest upload SQS Queue with tile information

    Note: This activity will clear the upload queue of any existing
          messages

    Args:
        args: {
            'job_id': '',
            'upload_queue': ARN,
            'ingest_queue': ARN,

            'resolution': 0,
            'project_info': [col_id, exp_id, ch_id],

            't_start': 0,
            't_stop': 0,
            't_tile_size': 0,

            'x_start': 0,
            'x_stop': 0,
            'x_tile_size': 0,

            'y_start': 0,
            'y_stop': 0,
            'y_tile_size': 0,

            'z_start': 0,
            'z_stop': 0,
            'z_tile_size': 0,

            'z_chunk_size': 64,  # or other possible number
            'MAX_NUM_ITEMS_PER_LAMBDA': 20000,
            'items_to_skip': 0    # number of chunks to skip
        }

    Returns:
        int: Number of messages put into the queue
    """
    print("Starting to populate upload queue for volumetric")
    pprint.pprint(args)

    queue = boto3.resource('sqs').Queue(args['upload_queue'])

    msgs = create_messages(args)
    sent = 0

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
            sent += len(resp['Successful'])

            if 'Failed' in resp and len(resp['Failed']) > 0:
                print("Batch failed to enqueue messages")
                print("Retries left: {}".format(retry))
                print("Boto3 send_messages response: {}".format(resp))
                time.sleep(SQS_RETRY_TIMEOUT)

                ids = [f['Id'] for f in resp['Failed']]
                batch = [b for b in batch if b['Id'] in ids]
                retry -= 1
                if retry == 0:
                    print("Exhausted retry count, stopping")
                    raise FailedToSendMessages(batch)  # SFN will relaunch the activity
                continue
            else:
                break

    return sent


def lookup_key_from_chunk_key(chunk_key):
    """
    breaks out the lookup_key from the chunk_key.

    Args:
        chunk_key (str): volumetric chunk key = hash&num_items&col_id&exp_id&ch_idres&x&y&z"

    Returns (str): lookup_key col_id&exp_id&ch_id

    """
    parts = chunk_key.split('&')
    lookup_parts = parts[2:5]
    return "&".join(lookup_parts)


def resolution_from_chunk_key(chunk_key):
    """
    breaks out the resolution from the chunk_key.

    Args:
        chunk_key (str): volumetric chunk key = hash&num_items&col_id&exp_id&ch_idres&x&y&z"

    Returns (str): resolution

    """
    parts = chunk_key.split('&')
    return parts[5]


def generate_object_key(lookup_key, resolution, time_sample, morton_id):
    """
    Generates an object key assumes iso=False.  Based on spdb.spatialdb.object.AWSObject.generate_object_key()

    Args:
        lookup_key (str): lookup key collection_id&experiment_id&channel_id
        resolution (int or str): resolution level of the object
        time_sample (int or str): time sample number
        morton_id (int or str): morton_id

    Returns:

    """
    base_key = '{}&{}&{}&{}'.format(lookup_key, resolution, time_sample, morton_id)

    # Hash
    hash_str = hashlib.md5(base_key.encode()).hexdigest()

    return "{}&{}".format(hash_str, base_key)


class LoopHelper:
    """
    Helper class that manages the triple nested loops that generate upload
    messages for the 3D space.  It adjusts the iteration ranges for the x and
    y axes for skipping the first n messages.  Messages are skipped because
    multiple instances of this lambda generate messages in parallel.
    """

    def __init__(self, args: dict):
        """
        Constructor.

        Args:
            args (dict): Same as args taken by handler().
        """
        self.args = args

        num_x_chunks = math.ceil((args["x_stop"] - args["x_start"]) / args["x_tile_size"])
        num_y_chunks = math.ceil((args["y_stop"] - args["y_start"]) / args["y_tile_size"])

        self.first_x_start = args["x_start"]
        self.first_y_start = args["y_start"]
        self.first_z_start = args["z_start"]

        msgs_to_skip = args['items_to_skip']
        if msgs_to_skip <= 0:
            return

        num_whole_xy_chunks_to_skip = int(msgs_to_skip / (num_y_chunks * num_x_chunks))
        self.first_z_start = args["z_start"] + num_whole_xy_chunks_to_skip * args["z_chunk_size"]

        msgs_to_skip -= num_whole_xy_chunks_to_skip * num_y_chunks * num_x_chunks
        if msgs_to_skip <= 0:
            return

        num_y_chunks_to_skip = int(msgs_to_skip / num_x_chunks)
        self.first_y_start = args["y_start"] + num_y_chunks_to_skip * args["y_tile_size"]

        msgs_to_skip -= num_y_chunks_to_skip * num_x_chunks
        if msgs_to_skip <= 0:
            return

        self.first_x_start = args["x_start"] + msgs_to_skip * args["x_tile_size"]

    def range_x(self, first: bool):
        if first:
            return range(self.first_x_start, self.args["x_stop"], self.args["x_tile_size"])
        return range(self.args["x_start"], self.args["x_stop"], self.args["x_tile_size"])

    def range_y(self, first: bool):
        if first:
            return range(self.first_y_start, self.args["y_stop"], self.args["y_tile_size"])
        return range(self.args["y_start"], self.args["y_stop"], self.args["y_tile_size"])

    def range_z(self):
        return range(self.first_z_start, self.args['z_stop'], self.args['z_chunk_size'])


def create_messages(args):
    """Create all of the tile messages to be enqueued.  Currently does not
    support t extent.

    Args:
        args (dict): Same arguments as handler()

    Returns:
        generator: Generator of strings containing Json data
    """

    tile_size = lambda v: args[v + "_tile_size"]  # noqa: E731

    # DP NOTE: generic version of
    # BossBackend.encode_chunk_key and BiossBackend.encode.tile_key
    # from ingest-client/ingestclient/core/backend.py
    def hashed_key(*args):
        base = '&'.join(map(str, args))

        md5 = hashlib.md5()
        md5.update(base.encode())
        digest = md5.hexdigest()

        return '&'.join([digest, base])

    try:
        loop_helper = LoopHelper(args)
    except ZeroDivisionError:
        print("Aborting, got divide by zero so no messages to generate")
        return

    # Currently, only allow ingest for time sample 0.
    t = 0
    msgs_emitted = 0
    x_first = True
    y_first = True
    for z in loop_helper.range_z():
        for y in loop_helper.range_y(y_first):
            y_first = False
            for x in loop_helper.range_x(x_first):
                x_first = False
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

                cuboids = []

                lookup_key = lookup_key_from_chunk_key(chunk_key)
                res = resolution_from_chunk_key(chunk_key)

                for chunk_offset_z in range(0, args["z_chunk_size"], CUBOID_Z):
                    for chunk_offset_y in range(0, tile_size('y'), CUBOID_Y):
                        for chunk_offset_x in range(0, tile_size('x'), CUBOID_X):
                            morton = XYZMorton(
                                [(x+chunk_offset_x)/CUBOID_X, (y+chunk_offset_y)/CUBOID_Y, (z+chunk_offset_z)/CUBOID_Z])
                            object_key = generate_object_key(lookup_key, res, t, morton)
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

                yield json.dumps(msg)

                msgs_emitted += 1
                if msgs_emitted >= args['MAX_NUM_ITEMS_PER_LAMBDA']:
                    return  # end the generator
