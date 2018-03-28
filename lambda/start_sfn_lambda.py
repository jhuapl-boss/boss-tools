# Lambda for starting step functions from within another step function.
# Step function tasks should catch KeyError and abort instead of retrying
# because the lambda will never succeed if a KeyError is raised.
#
# Inputs:
# {
#   "sfn_arn": "..."
# }
#
# Outputs (all inputs with the following additions):
# {
#   "sfn_output": {
#     "executionArn": "...",
#     "startDate": "..."
#   }
# }

import boto3
import json


def handler(event, context):
    sfn = boto3.client('stepfunctions')
    
    resp = sfn.start_execution(
        stateMachineArn=event['sfn_arn'],
        input=json.dumps(event)
    )

    start_date = resp.pop('startDate', None)
    if start_date is not None:
        # Make this JSON serializable.
        resp['startDate'] = str(start_date)

    event['sfn_output'] = resp

    return event

