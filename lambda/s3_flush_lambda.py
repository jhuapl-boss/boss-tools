import json
import boto3


def handler(event, context):
    for record in event['Records']:
        msg = json.loads(record['Sns']['Message'])

        action = msg['Event']
        if action == "autoscaling:TEST_NOTIFICATION":
            print("Test test, this is a test")
            return

