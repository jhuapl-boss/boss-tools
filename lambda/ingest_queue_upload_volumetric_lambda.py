from turtle import title
#import boto3
import json
import time
import hashlib
import pprint
import math
#from spdb.c_lib.ndlib import XYZMorton


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
            'y_stop': 0
            'y_tile_size': 0,

            'z_start': 0,
            'z_stop': 0
            'z_tile_size': 0,

            'z_chunk_size': 64, or other possible number
            'MAX_NUM_ITEMS_PER_LAMBDA': 20000
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
                    raise FailedToSendMessages(batch) # SFN will relaunch the activity
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

def test():
    print("Test")

def create_messages(args):
    """Create all of the tile messages to be enqueued.  Currently not support t extent.

    Args:
        args (dict): Same arguments as populate_upload_queue()

    Returns:
        list: List of strings containing Json data
    """

    # DP NOTE: generic version of
    # BossBackend.encode_chunk_key and BiossBackend.encode.tile_key
    # from ingest-client/ingestclient/core/backend.py
    def hashed_key(*args):
        base = '&'.join(map(str,args))

        md5 = hashlib.md5()
        md5.update(base.encode())
        digest = md5.hexdigest()

        return '&'.join([digest, base])

    tile_size = lambda v: args[v + "_tile_size"]

    # CF  New algorithm follows

    #  Goal is to PRE-COMPUTE the new starting values for T Z Y X , given # of tiles to skip
    #  Having the starting values will be more efficient than iterating for every TZYX

    # Tns:  T's new start position
    # Zns:  Z's new start position
    # Yns:  Y's new start position
    # Xns:  X's new start position

    #########################
    # 
    #  First, Let's create some Helper lambda functions 
    #

    # range helper function for X and Y dimensions
    new_range = lambda v, vns, First : range(vns, args[v + '_stop'] , args[v + '_tile_size']) if First else range(args[v + '_start'], args[v + '_stop'] , args[v + '_tile_size'])
    # range helper function for Z dimension
    new_range_z = lambda zns, First : range(zns, args['z_stop'] , args['z_chunk_size']) if First else range(args['z_start'], args['z_stop'] , args['z_chunk_size'])
    # range helper function for tiles
    new_range_tile = lambda t2s,z,First,nt : range(z + t2s, z + nt) if First else range(z, z + nt)

    # generic helper lambda func factoring tile_size (use ceiling)
    factor_ = lambda v: math.ceil((args[v + '_stop'] - args[v + '_start']) / args[v + '_tile_size'])

    # new start helper func (for tile_size cases)
    ns = lambda v,n: args[v + '_start'] + args[v + '_tile_size'] * n
    # new start helper func for special case Z (which uses chunk_size)
    ns_z = lambda v,n: args[v + '_start'] + args[v + '_chunk_size'] * n

    ##########################
    #
    # Now Let's start the actual calculations to compute the new ( Tns Znz Yns Xns ) values 
    #
    # first, factor in  (x, y, z dimensions)
    Xf = factor_('x')
    Yf = factor_('y')
    Zf = args['z_stop'] - args['z_start']

    # counter to keep track of # tiles to skip, used decrementally throughout the rest of the calcs below
    items_to_skip = args['items_to_skip']

    ############### T
    # T pos, factoring in X, Y, Z
    Tk = int(items_to_skip/(Xf*Yf*Zf))
    #compute T's new start
    Tns = ns('t',Tk)
    #decr tiles to skip
    items_to_skip -= Tk*Xf*Yf*Zf

    ############### Z
    # Z pos, factoring in X, Y
    Zk = int(items_to_skip/(args['z_chunk_size']*Xf*Yf))
    # compute Z's new start
    Zns = ns_z('z',Zk)
    # decr tiles to skip
    items_to_skip -= Zk*args['z_chunk_size']*Xf*Yf

    #set the number of tiles (to be used for X and Y calculations)
    num_tiles = min(args['z_chunk_size'], args['z_stop'] - Zns)

    ############### Y
    # Y pos, factoring in X
    Yk = int(items_to_skip/(num_tiles*Xf))
    # compute Y's new start pos
    Yns = ns('y',Yk)
    # decr tiles to skip
    items_to_skip -= Yk*num_tiles*Xf

    ############### X
    # X pos, factoring in num tiles
    Xk = int(items_to_skip/num_tiles)
    # compute X's new start pos
    Xns = ns('x',Xk)
    # decr tiles to skip
    items_to_skip -= Xk*num_tiles

    
    
    #chunks_to_skip = args['items_to_skip']
    #count_in_offset = 0

    # for t in range_('t'):
    #     for z in range(args['z_start'], args['z_stop'], args['z_chunk_size']):
    #         for y in range_('y'):
    #             for x in range_('x'):

    #                 if chunks_to_skip > 0:
    #                     chunks_to_skip -= 1
    #                     continue

    #                 if count_in_offset == 0:
    #                     print("Finished skipping chunks")

    #                 chunk_x = int(x / tile_size('x'))
    #                 chunk_y = int(y / tile_size('y'))
    #                 chunk_z = int(z / args['z_chunk_size'])
    #                 chunk_key = hashed_key(1,  # num of items
    #                                        args['project_info'][0],
    #                                        args['project_info'][1],
    #                                        args['project_info'][2],
    #                                        args['resolution'],
    #                                        chunk_x,
    #                                        chunk_y,
    #                                        chunk_z,
    #                                        t)

    #                 count_in_offset += 1
    #                 if count_in_offset > args['MAX_NUM_ITEMS_PER_LAMBDA']:
    #                     return  # end the generator

    #                 cuboids = []

    #                 # Currently, only allow ingest for time sample 0.
    #                 t = 0
    #                 lookup_key = lookup_key_from_chunk_key(chunk_key)
    #                 res = resolution_from_chunk_key(chunk_key)

    #                 for chunk_offset_z in range(0, args["z_chunk_size"], CUBOID_Z):
    #                     for chunk_offset_y in range(0, tile_size('y'), CUBOID_Y):
    #                         for chunk_offset_x in range(0, tile_size('x'), CUBOID_X):
    #                             #morton = XYZMorton(
    #                                 #[(x+chunk_offset_x)/CUBOID_X, (y+chunk_offset_y)/CUBOID_Y, (z+chunk_offset_z)/CUBOID_Z])
    #                             morton = '999999'
    #                             object_key = generate_object_key(lookup_key, res, t, morton)
    #                             new_cuboid = {
    #                                 "x": chunk_offset_x,
    #                                 "y": chunk_offset_y,
    #                                 "z": chunk_offset_z,
    #                                 "key": object_key
    #                             }
    #                             cuboids.append(new_cuboid)

    #                 msg = {
    #                     'chunk_key': chunk_key,
    #                     'cuboids': cuboids,
    #                 }

    #                 #yield json.dumps(msg)
    #                 return msg

if __name__ == "__main__":
    args = {
            'job_id': '',
            'upload_queue': 'ARN',
            'ingest_queue': 'ARN',

            'resolution': 0,
            'project_info': ['dyer', 'null', 'null'],

            't_start': 0,
            't_stop': 10,
            't_tile_size': 15,

            'x_start': 0,
            'x_stop': 10,
            'x_tile_size': 15,

            'y_start': 0,
            'y_stop': 10,
            'y_tile_size': 15,

            'z_start': 0,
            'z_stop': 10,
            'z_tile_size': 15,

            'z_chunk_size': 64, #or other possible number
            'MAX_NUM_ITEMS_PER_LAMBDA': 20000,
            'items_to_skip': 0  
        }
    print(create_messages(args))
