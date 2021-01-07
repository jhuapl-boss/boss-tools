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

import json
import unittest
from unittest.mock import call, patch
from copy import deepcopy

# lambdafcns contains symbolic links to lambda functions in boss-tools/lambda.
# Since lambda is a reserved word, this allows importing from that folder
# without updating scripts responsible for deploying the lambda code.
from lambdafcns.start_sfn_lambda import handler


class TestStartSfnLambda(unittest.TestCase):
    def test_missing_required_args(self):
        with self.assertRaises(KeyError):
            handler({"foo": "bar"}, None)

    def test_invoke_from_sfn(self):
        """Test the lambda as it would work when run as a step function state."""
        with patch(
            "lambdafcns.start_sfn_lambda.run_from_sfn", autospec=True
        ) as fake_run_from_sfn:
            event = {"sfn_arn": "fake_arn", "foo": "bar"}
            fake_resp = {"startDate": "now"}
            exp = deepcopy(event)
            exp["sfn_output"] = fake_resp
            fake_run_from_sfn.return_value = fake_resp

            actual = handler(event, None)
            self.assertEqual(exp, actual)

    def test_invoke_from_sqs(self):
        """Test the lambda as it would work when triggered via SQS."""
        with patch(
            "lambdafcns.start_sfn_lambda.run_from_sfn", autospec=True
        ) as fake_run_from_sfn:
            body = {"sfn_arn": "some_fake_arn", "foo": "bar"}
            event = {"Records": [{"body": json.dumps(body)}]}

            # No return value when triggered via SQS.
            handler(event, None)
            self.assertEqual(
                [call(body["sfn_arn"], body)], fake_run_from_sfn.mock_calls
            )
