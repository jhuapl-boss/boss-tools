# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

import unittest
from unittest.mock import patch, MagicMock 

class TestCaseWithPatchObject(unittest.TestCase):
    """
    Provide alternative way to setup mocks w/o need for decorator and extra
    arguments for each test method.
    """

    def patch_object(self, obj, name, **kwargs):
        """
        Setup mocks without need for decorator and extra arguments for each test.

        Args:
            obj (object): Object or module that will have one of its members mocked.
            name (str): Name of member to mock.
            kwargs (keyword args): Additional arguments passed to patch.object().

        Returns:
            (MagicMock)
        """
        patch_wrapper = patch.object(obj, name, autospec=True, **kwargs)
        magic_mock = patch_wrapper.start()
        # This ensures the patch is removed when the test is torn down.
        self.addCleanup(patch_wrapper.stop)
        return magic_mock
