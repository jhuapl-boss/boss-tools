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

import sys
sys.path.append("../")

try:
    from bossutils.keycloak import KeyCloakClient, KeyCloakError
    prefix = 'bossutils.'
except:
    from keycloak import KeyCloakClient, KeyCloakError
    prefix = '.'

from types import MethodType
import json
import unittest
from unittest import mock

import hvac

REALM = 'REALM'
BASE = 'http://auth'
TOKEN = {'access_token': 'access_token', 'refresh_token': 'refresh_token'}

def mock_login(self):
    self.token = TOKEN
    self.client_id = 'client_id'
    self.login_realm = 'realm'

def mock_logout(self):
    self.token = None
    self.client_id = None
    self.login_realm = None

class MockResponse:
    def __init__(self, status, text):
        self.status_code = status
        self.text = text

    def json(self):
        return json.loads(self.text)


class TestAuthentication(unittest.TestCase):
    """Test the login/logout methods and the context manager that wraps login/logout"""

    def setUp(self):
        self.client = KeyCloakClient(REALM, BASE)

    @mock.patch(prefix + 'keycloak.Vault', autospec = True)
    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_login(self, mPost, mVault):
        """Test that login() make the correct POST call with data from Vault and created the internal token variable"""
        mPost.return_value = MockResponse(200, json.dumps(TOKEN))
        mVault.return_value.read = lambda p, k: k

        self.assertIsNone(self.client.token)

        self.client.login()

        self.assertEqual(self.client.token, TOKEN)

        # DP NOTE: since the post call contains information read from Vault
        #          I don't explicitly check for the calls to Vault, because
        #          if they didn't happen, they the post call assert will fail

        url = BASE + '/realms/realm/protocol/openid-connect/token'
        data = {'grant_type': 'password',
                'client_id': 'client_id',
                'username': 'username',
                'password': 'password' }
        call = mock.call(url, data = data, verify = True)
        self.assertEqual(mPost.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.Vault', autospec = True)
    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_failed_login(self, mPost, mVault):
        """Test that if there is an error from Keycloak when logging in that a KeyCloakError is raised"""
        mPost.return_value = MockResponse(500, '[]')
        mVault.return_value.read = lambda p, k: k

        with self.assertRaises(KeyCloakError):
            self.client.login()

    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_logout(self, mPost):
        """Test that logout() makes the correct POST call with data used in login and clears the token variable"""
        mPost.return_value = MockResponse(200, '[]')

        self.client.login = MethodType(mock_login, self.client) # See comment in TestRequests.setUp()
        self.client.login()

        self.assertEqual(self.client.token, TOKEN)

        self.client.logout()

        self.assertIsNone(self.client.token)

        url = BASE + '/realms/realm/protocol/openid-connect/logout'
        data = {'refresh_token': 'refresh_token',
                'client_id': 'client_id' }
        call = mock.call(url, data = data, verify = True)
        self.assertEqual(mPost.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_failed_logout(self, mPost):
        """Test that if there is an error from Keycloak when logging out that a KeyCloakError is raised"""
        mPost.return_value = MockResponse(500, '[]')

        self.client.login = MethodType(mock_login, self.client) # See comment in TestRequests.setUp()
        self.client.login()

        with self.assertRaises(KeyCloakError):
            self.client.logout()

    def test_with(self):
        """Test that that when in a 'using' block that login and logout are called"""
        self.client.loginCalled = False
        self.client.logoutCalled = False

        def call_login(self):
            self.loginCalled = True

        def call_logout(self):
            self.logoutCalled = True

        self.client.login = MethodType(call_login, self.client)
        self.client.logout = MethodType(call_logout, self.client)

        with self.client as kc:
            pass

        self.assertTrue(self.client.loginCalled)
        self.assertTrue(self.client.logoutCalled)

    def test_failed_with(self):
        """Test that if there is an exception in a 'using' block that it is propogated correctly"""
        self.client.loginCalled = False
        self.client.logoutCalled = False

        def call_login(self):
            self.loginCalled = True

        def call_logout(self):
            self.logoutCalled = True

        self.client.login = MethodType(call_login, self.client)
        self.client.logout = MethodType(call_logout, self.client)

        with self.assertRaises(Exception):
            with self.client as kc:
                raise Exception()

        self.assertTrue(self.client.loginCalled)
        self.assertTrue(self.client.logoutCalled)

    def test_failed_with_login(self):
        """Test that when in a 'using' block if login() fails logout() is not called"""
        self.client.loginCalled = False
        self.client.logoutCalled = False

        def call_login(self):
            self.loginCalled = True
            raise Exception()

        def call_logout(self):
            self.logoutCalled = True

        self.client.login = MethodType(call_login, self.client)
        self.client.logout = MethodType(call_logout, self.client)

        with self.assertRaises(Exception):
            with self.client as kc:
                pass

        self.assertTrue(self.client.loginCalled)
        self.assertFalse(self.client.logoutCalled)

    def test_failed_with_logout(self):
        """Test that when in a 'using' block if logout() fails no exception is propogated"""
        self.client.loginCalled = False
        self.client.logoutCalled = False

        def call_login(self):
            self.loginCalled = True

        def call_logout(self):
            self.logoutCalled = True
            raise Exception()

        self.client.login = MethodType(call_login, self.client)
        self.client.logout = MethodType(call_logout, self.client)

        with self.client as kc:
            pass

        self.assertTrue(self.client.loginCalled)
        self.assertTrue(self.client.logoutCalled)

class TestRequests(unittest.TestCase):
    """Tests the internal get/post/put/delete methods"""

    def setUp(self):
        """Create the KeyCloakClient object and override login/logout with mock versions"""
        self.client = KeyCloakClient(REALM, BASE)

        # Have to use MethodType to bind the mock method to the specific instance
        # of the KeyCloakClient instead of binding it to all instances of the
        # class
        # See http://stackoverflow.com/questions/972/adding-a-method-to-an-existing-object-instance
        self.client.login = MethodType(mock_login, self.client)
        self.client.logout = MethodType(mock_logout, self.client)

    # DP TODO: the test_(get/post/put/delete) test logic can be merged into a
    #          single unified method implementing most of the logic
    @mock.patch(prefix + 'keycloak.requests.get', autospec = True)
    def test_get(self, mGet):
        """Test that the appropriate URL, headers, and parameters are set"""
        mGet.return_value = MockResponse(200, '[]')
        suffix = 'test'

        with self.client as kc:
            response = kc._get(suffix, {'param': 'one'}, {'header': 'one'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

        url = BASE + '/admin/realms/' + REALM + '/' + suffix
        params = {'param': 'one'}
        headers = {'header': 'one', 'Authorization': 'Bearer access_token'}
        call = mock.call(url, headers = headers, params = params, verify = True)
        self.assertEqual(mGet.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.requests.get', autospec = True)
    def test_failed_get(self, mGet):
        """Test that when there is a HTTP error response code that a KeyCloakError is raised"""
        mGet.return_value = MockResponse(500, '[]')

        with self.assertRaises(KeyCloakError):
            with self.client as kc:
                response = kc._get('test')

    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_post(self, mPost):
        """Test that the appropriate URL, headers, and data are set"""
        mPost.return_value = MockResponse(200, '[]')
        suffix = 'test'

        with self.client as kc:
            response = kc._post(suffix, {'data': 'one'}, {'header': 'one'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

        url = BASE + '/admin/realms/' + REALM + '/' + suffix
        data = {'data': 'one'}
        headers = {'header': 'one',
                   'Authorization': 'Bearer access_token',
                   'Content-Type': 'application/json'}
        call = mock.call(url, headers = headers, data = data, verify = True)
        self.assertEqual(mPost.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.requests.post', autospec = True)
    def test_failed_post(self, mPost):
        """Test that when there is a HTTP error response code that a KeyCloakError is raised"""
        mPost.return_value = MockResponse(500, '[]')

        with self.assertRaises(KeyCloakError):
            with self.client as kc:
                response = kc._post('test')

    @mock.patch(prefix + 'keycloak.requests.put', autospec = True)
    def test_put(self, mPut):
        """Test that the appropriate URL, headers, and data are set"""
        mPut.return_value = MockResponse(200, '[]')
        suffix = 'test'

        with self.client as kc:
            response = kc._put(suffix, {'data': 'one'}, {'header': 'one'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

        url = BASE + '/admin/realms/' + REALM + '/' + suffix
        data = {'data': 'one'}
        headers = {'header': 'one',
                   'Authorization': 'Bearer access_token',
                   'Content-Type': 'application/json'}
        call = mock.call(url, headers = headers, json = data, verify = True)
        self.assertEqual(mPut.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.requests.put', autospec = True)
    def test_failed_put(self, mPut):
        """Test that when there is a HTTP error response code that a KeyCloakError is raised"""
        mPut.return_value = MockResponse(500, '[]')

        with self.assertRaises(KeyCloakError):
            with self.client as kc:
                response = kc._put('test')

    @mock.patch(prefix + 'keycloak.requests.delete', autospec = True)
    def test_delete(self, mDelete):
        """Test that the appropriate URL, headers, and data are set"""
        mDelete.return_value = MockResponse(200, '[]')
        suffix = 'test'

        with self.client as kc:
            response = kc._delete(suffix, {'data': 'one'}, {'header': 'one'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

        url = BASE + '/admin/realms/' + REALM + '/' + suffix
        data = {'data': 'one'}
        headers = {'header': 'one',
                   'Authorization': 'Bearer access_token',
                   'Content-Type': 'application/json'}
        call = mock.call(url, headers = headers, data = data, verify = True)
        self.assertEqual(mDelete.mock_calls, [call])

    @mock.patch(prefix + 'keycloak.requests.delete', autospec = True)
    def test_failed_delete(self, mDelete):
        """Test that when there is a HTTP error response code that a KeyCloakError is raised"""
        mDelete.return_value = MockResponse(500, '[]')

        with self.assertRaises(KeyCloakError):
            with self.client as kc:
                response = kc._delete('test')

class TestClient(unittest.TestCase):
    """Test the client methods used to interact with Keycloak"""

    def setUp(self):
        """Create the KeyCloakClient object and override previously tested internal methods with mock versions"""
        self.client = KeyCloakClient(REALM, BASE)

        # DP NOTE: login / logout are not mocked, because the methods are called without logging in or out

    @staticmethod
    def makeMM(arg):
        """Make a mock object and set the return value(s)

        Args:
            arg : Vale or list of Values for mock object to return when called

        Returns:
            mock.MagicMock : mock object
        """

        mm = mock.MagicMock()
        if type(arg) == type([]):
            mm.side_effect = arg
        else:
            mm.return_value = arg

        return mm

    def setReturn(self, get=None, post=None, put=None, delete=None, get_user_id=False, get_role_by_name=False):
        """Mock up the return values for the different HTTP methods of KeyCloakClient

        Args:
            get : Value or Iterable of values to return for GETs
            post : Value or Iterable of values to return for POSTs
            put : Value or Iterable of values to return for PUTs
            delete : Value or Iterable of values to return for DELETEs
            get_user_id (bool) : If KeyCloakClient.get_user_id should be stubbed out
            get_role_by_name (bool) : If KeyCloakClient.get_role_by_name should be stubbed out
        """
        make = lambda m: TestClient.makeMM(m)

        self.client._get = make(get)
        self.client._post = make(post)
        self.client._put = make(put)
        self.client._delete = make(delete)

        if get_user_id:
            self.client.get_user_id = make(0)

        if get_role_by_name:
            self.client.get_role_by_name = make({})

    def test_get_userdata(self):
        username = 'test'
        data = {'username': username}
        self.setReturn(get=MockResponse(200, json.dumps(data)), get_user_id=True)

        response = self.client.get_userdata(username)

        self.assertEqual(data, response)

        url = 'users/0' # user id set in setReturn
        call = mock.call(url)
        self.assertEqual(self.client._get.mock_calls, [call])
        self.assertEqual(self.client.get_user_id.call_count, 1)

    def test_get_userinfo(self):
        data = {'test': 'test'}
        self.setReturn(get=MockResponse(200, json.dumps(data)))

        response = self.client.get_userinfo()

        self.assertEqual(data, response)

        url = 'protocol/openid-connect/userinfo'
        call = mock.call(url)
        self.assertEqual(self.client._get.mock_calls, [call])

    def test_create_user(self):
        data = {'test': 'test'}
        self.setReturn()

        response = self.client.create_user(data)

        self.assertIsNone(response)

        call = mock.call('users', data)
        self.assertEqual(self.client._post.mock_calls, [call])

    def test_reset_password(self):
        data = {'test': 'test'}
        self.setReturn(get_user_id = True)

        response = self.client.reset_password('test', data)

        self.assertIsNone(response)

        call = mock.call('users/0/reset-password', data)
        self.assertEqual(self.client._put.mock_calls, [call])
        self.assertEqual(self.client.get_user_id.call_count, 1)

    def test_delete_user(self):
        self.setReturn(get_user_id = True)

        response = self.client.delete_user('test')

        self.assertIsNone(response)

        call = mock.call('users/0')
        self.assertEqual(self.client._delete.mock_calls, [call])
        self.assertEqual(self.client.get_user_id.call_count, 1)

    def test_get_user_id(self):
        username = 'test'
        params = {'username': username}
        self.setReturn(get=MockResponse(200, '[{"id": 0}]'))

        response = self.client.get_user_id(username)

        self.assertEqual(0, response)

        call = mock.call('users', params)
        self.assertEqual(self.client._get.mock_calls, [call])

    def test_failed_get_user_id(self):
        username = 'test'
        params = {'username': username}
        self.setReturn(get=MockResponse(500, '[]'))

        with self.assertRaises(KeyCloakError):
            self.client.get_user_id(username)

        call = mock.call('users', params)
        self.assertEqual(self.client._get.mock_calls, [call])

    def test_get_realm_roles(self):
        data = {'test': 'test'}
        self.setReturn(get=MockResponse(200, json.dumps(data)), get_user_id=True)

        response = self.client.get_realm_roles('test')

        self.assertEqual(data, response)

        call = mock.call('users/0/role-mappings/realm')
        self.assertEqual(self.client._get.mock_calls, [call])
        self.assertEqual(self.client.get_user_id.call_count, 1)

    def test_get_role_by_name(self):
        data = {'test': 'test'}
        self.setReturn(get=MockResponse(200, json.dumps(data)))

        response = self.client.get_role_by_name('test')

        self.assertEqual(data, response)

        call = mock.call('roles/test')
        self.assertEqual(self.client._get.mock_calls, [call])

    def test_failed_get_role_by_name(self):
        self.setReturn(get=MockResponse(500, '[]'))

        with self.assertRaises(KeyCloakError):
            self.client.get_role_by_name('test')

        call = mock.call('roles/test')
        self.assertEqual(self.client._get.mock_calls, [call])

    def test_map_role_to_user(self):
        self.setReturn(get_user_id = True, get_role_by_name = True)

        response = self.client.map_role_to_user('test', 'test')

        self.assertIsNone(response)

        call = mock.call('users/0/role-mappings/realm', '[{}]')
        self.assertEqual(self.client._post.mock_calls, [call])

    def test_remove_role_from_user(self):
        self.setReturn(get_user_id = True, get_role_by_name = True)

        response = self.client.remove_role_from_user('test', 'test')

        self.assertIsNone(response)

        call = mock.call('users/0/role-mappings/realm', '[{}]')
        self.assertEqual(self.client._delete.mock_calls, [call])
