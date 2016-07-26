#!/usr/bin/env python3.4
print("in s3_flush_lambda")
import bossutils
import spdb
from spdb.spatialdb import state
print("finished part1 imports")
user_data = bossutils.utils.read_url("http://169.254.169.254/latest/user-data")
print("finished loading user_data")
cache_state = user_data["aws"]["cache-state"]
cache_state_db = user_data["aws"]["cache-state-db"]
my_state = state.CacheStateDB({ "cache_state_host": cache_state, "cache_state_db": cache_state_db })
my_state.add_cache_misses(["6","4","3"])
print("finished part1")

print("checking c_lib")
from spdb.c_lib import ndlib
print("finished c_lib imports.")
id = ndlib.MortonXYZ(10)
print("results:")
for w in id:
   print(str(w))
print("finished part2")