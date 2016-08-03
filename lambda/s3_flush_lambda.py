#!/usr/bin/env python3.4
# This lambda is for s3_flush for the cache
#
# It expects to get from events dictionary
# {
#   "lambda-name": "s3_flush",
#   "cache-state": "cache-state.hiderrt1.boss",
#   "cache-state-db": "0"
#   "s3-flush-queue": "https://sqs.us-east-1.amazonaws.com/256215146792/S3flushHiderrt1Boss"
#   "s3-flush-deadletter-queue": "https://sqs.us-east-1.amazonaws.com/256215146792/DeadletterHiderrt1Boss"
# }
#



print("in s3_flush_lambda")
import bossutils
import spdb
import sys
import json
from spdb.spatialdb import state
print("finished part1 imports")

json_event = sys.argv[1]
event = json.loads(json_event)


my_state = state.CacheStateDB({ "cache_state_host": event["cache-state"],
                                "cache_state_db": int(str(event["cache-state-db"])) })
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
