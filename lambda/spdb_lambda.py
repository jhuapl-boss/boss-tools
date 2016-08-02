#!/usr/bin/env python3.4
# This lambda tests that it can read from user-data, and access the cache_state_db
# then import spdb and access the compiled c_lib
#
# {
#   "lambda-name": "test",
# }
#
print("in test_lambda")
import bossutils
import spdb
from spdb.spatialdb import state
print("finished part1 imports")

print("checking c_lib")
from spdb.c_lib import ndlib
print("finished c_lib imports.")
id = ndlib.MortonXYZ(10)
print("results:")
for w in id:
   print(str(w))
print("finished part2")
