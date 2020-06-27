# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bossutils.vault import Vault, VAULT_SECTION, VAULT_URL_KEY, VAULT_TOKEN_KEY
import hvac
import unittest
from unittest.mock import patch

# The patch decorator mocks hvac.Client and passes it as the second parameter
# to each test method.  Setting autospec true forces the signature of the
# mock's methods match that of hvac.Client.
@patch('hvac.Client', autospec=True)
class TestVaultClient(unittest.TestCase):
    def setUp(self):
        """ Create a dummy configuration. """
        self.cfg = { VAULT_SECTION :
            {
                VAULT_URL_KEY : 'https://foo.com',
                VAULT_TOKEN_KEY : 'open'
            }
        }

        patch_wrapper = patch('bossutils.vault.utils.read_url')
        magic_mock = patch_wrapper.start()
        magic_mock.return_value = ''
        # This ensures the patch is removed when the test is torn down.
        self.addCleanup(patch_wrapper.stop)

    def test_exception_if_cant_auth_to_vault(self, mockClient):
        instance = mockClient.return_value
        instance.is_authenticated.return_value = False
        with self.assertRaises(Exception):
            Vault(self.cfg)

    def test_logout_destroys_hvac_client(self, mockClient):
        v = Vault(self.cfg)
        v.logout()
        self.assertIsNone(v.client)

    def test_exception_if_read_dict_fails(self, mockClient):
        instance = mockClient.return_value
        instance.read.return_value = None
        v = Vault(self.cfg)
        with self.assertRaises(Exception):
            v.read_dict('secrets')

    def test_exception_if_read_fails(self, mockClient):
        instance = mockClient.return_value
        instance.read.return_value = None
        v = Vault(self.cfg)
        with self.assertRaises(Exception):
            v.read('secrets', 'super')

    def test_exception_if_key_dosent_exist(self, mockClient):
        instance = mockClient.return_value
        instance.read.return_value = { "data": {} }
        v = Vault(self.cfg)
        with self.assertRaises(Exception):
            v.read('secrets', 'super')

    def test_retry_logic_calls_login(self, mockClient):
        instance = mockClient.return_value
        v = Vault(self.cfg)
        instance.read.side_effect = [hvac.exceptions.Forbidden('Token has expired'),{ "data" : {"super": ""} }]
        with patch.object(Vault, 'login') as mock:
            v.read('secrets','super')
            mock.assert_called_once_with(v)

    def test_retry_logic_returns(self, mockClient):
        instance = mockClient.return_value
        v = Vault(self.cfg)
        instance.read.side_effect = [hvac.exceptions.Forbidden('Token has expired'),{ "data" : {"super": "this!"} }]
        with patch.object(Vault, 'login'):
            self.assertEqual(v.read('secrets', 'super'), "this!")
