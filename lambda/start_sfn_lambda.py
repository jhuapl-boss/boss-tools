# Copyright 2021 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Lambda for starting step functions from within another step function OR
via from an SQS message when the lambda is connected to an SQS queue.
Step function tasks should catch KeyError and abort instead of retrying
because the lambda will never succeed if a KeyError is raised.

See the AWS documentation for a full example of what the event data looks like
from an SQS queue: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html

Inputs:
{
  "sfn_arn": "..."
  # OR
  "Records": [{ ...
                body: "{\"sfn_arn\": \"the sfn ARN\" ... }"
              }
             ]
}

Outputs (when invoked by a step function, all inputs with the following
additions; no output when invoked by SQS):
{
  "sfn_output": {
    "executionArn": "...",
    "startDate": "..."
  }
}
"""

import boto3
import json


def handler(event, context):
    if "Records" in event and len(event["Records"]) > 0:
        if "body" not in event["Records"][0]:
            raise KeyError("No body found in SQS message")
        body = json.loads(event["Records"][0]["body"])
        if "sfn_arn" not in body:
            raise KeyError("SQS message does not contain 'sfn_arn'")
        start_sfn(body["sfn_arn"], body)
        return

    if "sfn_arn" in event:
        resp = start_sfn(event["sfn_arn"], event)
        event["sfn_output"] = resp
        return event

    raise KeyError("event must contain 'sfn_arn' or 'Records[0]'")


def start_sfn(arn, sfn_args):
    """
    Start the step function.

    Args:
        arn (str): step function ARN.
        sfn_args (dict): Arguments passed to step function.

    Returns:
        (dict): Response from boto3 start_execution() + 'startDate'.
    """
    sfn = boto3.client("stepfunctions")

    resp = sfn.start_execution(stateMachineArn=arn, input=json.dumps(sfn_args))

    start_date = resp.pop("startDate", None)
    if start_date is not None:
        # Make this JSON serializable.
        resp["startDate"] = str(start_date)

    return resp
