#!/usr/bin/env python3.4
# Page in cuboid from S3 to the cache.
#
# Expects these keys from the events dictionary:
# {
#   'kv_config': {...},
#   'state_config': {...},
#   'object_store_config': {...},
#   'object_key': '...',
#   'page_in_channel': '...'
# }

print("in s3_to_cache lambda")
import json
import sys
from spdb.spatialdb import SpatialDB

# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

# Setup SPDB instance
sp = SpatialDB(event['kv_config'],
               event['state_config'],
               event['object_store_config'])

object_key = event['object_key']
page_in_channel = event['page_in_channel']

cube_bytes = sp.objectio.get_single_object(object_key)
cache_keys = sp.objectio.object_to_cached_cuboid_keys([object_key])
sp.kvio.put_cubes(cache_keys[0], [cube_bytes])
if page_in_channel is not None:
    sp.cache_state.notify_page_in_complete(page_in_channel, object_key)
