#!/usr/bin/env python3.4
# This lambda is used for testing that lambda are firing from multi-lambda
#
# It expects to get from events dictionary
# {
#   "lambda-name": "simple_lambda",
# }

import sys
import json
import logging


# Parse input args passed as a JSON string from the lambda loader
json_event = sys.argv[1]
event = json.loads(json_event)

log = logging.getLogger()
log.setLevel(logging.INFO)

print (sys.version)

print("test_lambda fired.")
log.info("test_lambda fired.")
