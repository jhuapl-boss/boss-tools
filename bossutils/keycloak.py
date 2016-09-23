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

"""Client and general exception for working with Keycloak Users / Roles

Author:
    Derek Pryor <Derek.Pryor@jhuapl.edu>
"""
import json
import requests
from bossutils.vault import Vault
from bossutils.logger import BossLogger

LOG = BossLogger().logger

class KeyCloakError(Exception):
    def __init__(self, status, data):
        super(KeyCloakError, self).__init__(data)
        self.status = status
        self.data = data

    @staticmethod
    def _get_message(res):
        try: # Assume json formatted data
            return json.dumps(res.json())
        except:
            try: # Try just raw response
                return res.text
            except:
                return None

    @classmethod
    def raise_for_status(cls, res):
        if 400 <= res.status_code <= 600: # handle both Client and Server errors
            msg = cls._get_message(res)
            raise cls(res.status_code, msg)

class KeyCloakClient:
    """Client for connecting to Keycloak and using the REST API.

    Client provides a method for issuing requests to the Keycloak REST API and
    a set of methods to simplify Keycloak configuration.
    """

    # DP TODO: standardize on all inputs / output being json dictionaries, not encoded strings
    # DP ???: have all function arguments be **kwargs instead of passing in dictionaries?

    def __init__(self, realm, url_base=None, https=True, verify_ssl=True):
        """KeyCloakClient constructor

        Args:
            url_base (string) : The base URL to prepend to all request URLs
            verify_ssl (bool) : Whether or not to verify HTTPS certs
        """
        if url_base:
            self.url_base = url_base
        else:
            self.url_base = 'http://auth:8080/auth'

        self.realm = realm
        self.token = None
        self.https = https
        self.verify_ssl = verify_ssl
        self.vault_path = "secret/keycloak"

    def login(self, username=None, password=None, client_id=None, login_realm=None):
        """Log the user in by Retrieve tokens for the user with the specified username and password.
        Args:
            username (str): login username
            password (str): login password
            client_id (str): OIDC client_id for logging into server
            login_realm (str): Keycloak realm of the login user

        Returns:
            requests.Response: token endpoint response

        Note: Login realm can be different from the realm that the client is used to manage
        """
        # Get the password from vault
        vault = Vault()
        if username is None:
            username = vault.read(self.vault_path, 'username')
            password = vault.read(self.vault_path, 'password')

        # Save the client_id and login realm for logging out
        self.client_id = client_id or vault.read(self.vault_path, 'client_id')
        self.login_realm = login_realm or vault.read(self.vault_path, 'realm')

        url = '{}/realms/{}/protocol/openid-connect/token'.format(self.url_base, self.login_realm)
        data = {
            'grant_type': 'password',
            'client_id': self.client_id,
            'username': username,
            'password': password,
        }
        response = requests.post(url, data=data, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)
        self.token = response.json()

        return response

    def logout(self):
        """Logout from Keycloak.

        Logout will invalidate the Keycloak session and clean the local token (
        self.token)
        """
        if self.token is None:
            return

        token_endpoint = '{}/realms/{}/protocol/openid-connect/logout'.format(self.url_base, self.login_realm)
        data = {
            "refresh_token": self.token["refresh_token"],
            "client_id": self.client_id,
        }
        response = requests.post(token_endpoint, data=data, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)
        self.token = None
        self.client_id = None
        self.login_realm = None

    def __enter__(self):
        """The start of the context manager, which handles login / logout from Keycloak."""
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """The end of the context manager. Log any error when trying to logut and
        propogate any exception that happened while the context was active."""
        try:
            self.logout()
        except:
            LOG.exception("Error logging out of Keycloak")

        if exc_type is None:
            return None
        else:
            return False # don't supress the exception

    def _get(self, url_suffix = "", params = None, headers = {}):
        url = "{}/admin/realms/{}/{}".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']

        response = requests.get(url, headers=headers, params=params, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)

        return response

    def _post(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/{}".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']
        headers['Content-Type'] = 'application/json'

        response = requests.post(url, headers=headers, data=data, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)

        return response

    def _put(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/{}".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']
        headers['Content-Type'] = 'application/json'

        response = requests.put(url, headers=headers, json=data, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)

        return response

    def _delete(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/{}".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']
        headers['Content-Type'] = 'application/json'

        response = requests.delete(url, headers=headers, data=data, verify=self.https and self.verify_ssl)
        KeyCloakError.raise_for_status(response)

        return response

    def get_userdata(self, username):
        userid = self.get_user_id(username)
        url = "users/{}".format(userid)

        return self._get(url).json()

    def get_userinfo(self):
        """Retrieve user info corresponding to the bearer_token from the 'userinfo endpoint'.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        url = 'protocol/openid-connect/userinfo'

        return self._get(url).json()

    def create_user(self, user_data):
        """Create a new user in the '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        url = 'users'

        self._post(url, user_data)

    def reset_password(self, user_name, credentials):
        """Reset password for a user.
        Args:
           user_name (str): Username of the user
        Returns:
            requests.Response: userinfo endpoint response
        """
        user_id = self.get_user_id(user_name)
        url = 'users/{}/reset-password'.format(user_id)

        self._put(url, credentials)

    def delete_user(self, user_name):
        """Delete a user from keycloak'.
        Args:
            user_name : User name of the user to be deleted
        Returns:
            requests.Response: userinfo endpoint response
        """
        userid = self.get_user_id(user_name)
        url = 'users/{}'.format(userid)

        self._delete(url)

    def get_user_id(self, username):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = 'users'
        params = {
            'username': username
        }

        response = self._get(url, params)
        data = response.json()
        if len(data) > 0:
            return response.json()[0]['id']
        else:
            raise KeyCloakError(404, {"error": "User not found"})

    def get_realm_roles(self, username):
        """Retrieve all assigned realm roles from keycloak.

        Returns:
            JSON RoleRepresentation array
        """
        id = self.get_user_id(username)
        url = 'users/{}/role-mappings/realm'.format(id)

        return self._get(url).json()

    def get_role_by_name(self, rolename):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = 'roles/{}'.format(rolename)

        response = self._get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise KeyCloakError(404, {"error": "Role not found"})

    def map_role_to_user(self, username, rolename):
        """Map a role to user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        id = self.get_user_id(username)
        role = self.get_role_by_name(rolename)
        url = 'users/{}/role-mappings/realm'.format(id)
        roles = json.dumps([role])

        self._post(url, roles)

    def remove_role_from_user(self, username, rolename):
        """Remove a mapped role from a user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        id = self.get_user_id(username)
        role = self.get_role_by_name(rolename)
        url = 'users/{}/role-mappings/realm'.format(id)
        roles = json.dumps([role])

        self._delete(url, roles)
