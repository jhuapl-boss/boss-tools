import json
import os
import subprocess
LAMBDA_PATH_PREFIX = "local/lib/python3.4/site-packages/lambda/"

# List of supported Lambda functions
lambda_dictionary = {"s3_flush": LAMBDA_PATH_PREFIX + "s3_flush_lambda.py",
                     "page_in_lambda_function": LAMBDA_PATH_PREFIX + "s3_to_cache.py",
                     "tile_upload": LAMBDA_PATH_PREFIX + "tile_upload_lambda.py",
                     "ingest": LAMBDA_PATH_PREFIX + "ingest_lambda.py",
                     "upload_enqueue": LAMBDA_PATH_PREFIX + "upload_enqueue_lambda.py",
                     "delete_lambda": LAMBDA_PATH_PREFIX + "delete_lambda.py",
                     "test_lambda": LAMBDA_PATH_PREFIX + "test_lambda.py",
                     "test": LAMBDA_PATH_PREFIX + "spdb_lambda.py"}


def handler(event, context):
    # Check for a "lambda-name" setting
    print(event)
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

    json_event = json.dumps(event)
    print("event: " + json_event)
    args = ("bin/python3.4", lambda_path, json_event)

    # AWS defines a PYTHONPATH that breaks our Python 3.4 code.  We can't
    # import concurrent.futures, specifically.
    env = os.environ
    if "PYTHONPATH" in env:
        del env['PYTHONPATH']

    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    popen.wait()
    output = popen.stdout.read()
    err_str = popen.stderr.read()
    print("output: " + output)
    print("err: " + err_str)
