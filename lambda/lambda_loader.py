import json
import boto3
import subprocess
LAMBDA_PATH_PREFIX = "local/lib/python3.4/site-packages/lambda/"
lambda_dictionary = { "s3_flush": LAMBDA_PATH_PREFIX + "s3_flush_lambda.py",
                      "page_in_lambda_function": LAMBDA_PATH_PREFIX + "s3_to_cache.py",
                      "test": LAMBDA_PATH_PREFIX + "spdb_lambda.py" }

def handler(event, context):
    lambda_name = event["lambda-name"]
    if lambda_name is None:
        print("No lambda_name given")
        exit(1)
    lambda_path = lambda_dictionary[lambda_name]
    if lambda_path is None:
        print("No path found for lambda: " + lambda_name)
        exit(2)

    json_event = json.dumps(event)
    print("event: " + json_event)
    args = ("bin/python3.4", lambda_path, json_event)
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    popen.wait()
    output = popen.stdout.read()
    err_str = popen.stderr.read()
    print("output: " + output)
    print("err: " + err_str)
