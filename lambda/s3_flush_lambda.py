#!/usr/bin/env python3.4
print("in s3_flush_lambda")
import spdb
from spdb.spatialdb import state
print("finished imports")

my_state = state.CacheStateDB({ "cache_state_host": "cache-state.hiderrt1.boss", "cache_state_db": 0 })
my_state.add_cache_misses(["6","4","3"])
print("finished.")
