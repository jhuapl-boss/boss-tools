import json
import boto3
import subprocess


def handler(event, context):
    args = ("bin/python3.4", "local/lib/python3.4/site-packages/lambda/spdb_lambda.py")
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    popen.wait()
    output = popen.stdout.read()
    err_str = popen.stderr.read()
    print("output: " + output)
    print("err: " + err_str)


