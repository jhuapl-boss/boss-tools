from bossutils.vault import *
import hvac
import unittest
from unittest.mock import patch

# The patch decorator mocks hvac.Client and passes it as the second parameter
# to each test method.  Setting autospec true forces the signature of the
# mock's methods match that of hvac.Client.
@patch('hvac.Client', autospec = True)
class TestVaultClient(unittest.TestCase):
    def setUp(self):
        """ Create a dummy configuration. """
        self.cfg = { VAULT_SECTION :
            {
                VAULT_URL_KEY : 'https://foo.com',
                VAULT_TOKEN_KEY : 'open'
            }
        }

    def test_exception_if_cant_auth_to_vault(self, mockClient):
        instance = mockClient.return_value
        instance.is_authenticated.return_value = False
        with self.assertRaises(Exception):
            v = Vault(self.cfg)

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
