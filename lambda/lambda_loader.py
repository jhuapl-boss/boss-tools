import json
import os
import subprocess
import sys
import logging
import runpy

LAMBDA_PATH_PREFIX = "lambda/"

log = logging.getLogger()
log.setLevel(logging.ERROR)

# List of supported Lambda functions
lambda_dictionary = {"s3_flush": LAMBDA_PATH_PREFIX + "s3_flush_lambda.py",
                     "page_in_lambda_function": LAMBDA_PATH_PREFIX + "s3_to_cache.py",
                     "tile_upload": LAMBDA_PATH_PREFIX + "tile_upload_lambda.py",
                     "ingest": LAMBDA_PATH_PREFIX + "ingest_lambda.py",
                     "upload_enqueue": LAMBDA_PATH_PREFIX + "upload_enqueue_lambda.py",
                     "delete_lambda": LAMBDA_PATH_PREFIX + "delete_lambda.py",
                     "simple_lambda": LAMBDA_PATH_PREFIX + "simple_lambda.py",
                     "test": LAMBDA_PATH_PREFIX + "spdb_lambda.py"}


def handler(event, context):
    # Check for a "lambda-name" setting
    log.debug("starting lambda_loader -> handler()")

    if "lambda-name" not in event:
        # Check if this is an S3 event which can be assumed to be ingest uploading
        if "Records" in event:
            if "eventSource" in event["Records"][0]:
                if event["Records"][0]["eventSource"] == "aws:s3":
                    lambda_name = "tile_upload"
            else:
                print("No lambda-name given")
                exit(1)
    else:
        lambda_name = event["lambda-name"]

    # Load lambda details and launch
    lambda_path = lambda_dictionary[lambda_name]
    if lambda_path is None:
        print("No path found for lambda: " + lambda_name)
        exit(2)

    log.debug("got lambda path")
    json_event = json.dumps(event)
    print("event: " + json_event)

    # Pass to lambda function we're about to load.
    if len(sys.argv) > 1:
        sys.argv[1] = json_event
    else:
        sys.argv.append(json_event)
    runpy.run_path(lambda_path)
