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

"""Library for common methods that are used by the different configs scripts.

Library currently appends the boss-manage.git/vault/ directory to the system path
so that it can import vault/bastion.py and vault/vault.py.

Library contains a set of AWS lookup methods for locating AWS data and other related
helper functions and classes.

Author:
    Derek Pryor <Derek.Pryor@jhuapl.edu>
"""
import json
import requests
from bossutils.vault import Vault
from bossutils.logger import BossLogger

LOG = BossLogger().logger

class KeyCloakClient:
    """Client for connecting to Keycloak and using the REST API.

    Client provides a method for issuing requests to the Keycloak REST API and
    a set of methods to simplify Keycloak configuration.
    """

    # DP TODO: standardize on all inputs / output being json dictionaries, not encoded strings
    # DP ???: have all function arguments be **kwargs instead of passing in dictionaries?

    # DP TODO: pull realm from vault
    # DP TODO: pull login / logout client id from vault
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

    def login(self, client_id=None, username=None, password=None):
        """Log the user in by Retrieve tokens for the user with the specified username and password.
        Args:
            client_id (str): client identifier
            username (str): login username
            password (str): login password
        Returns:
            requests.Response: token endpoint response
        """
        # Get the password from vault
        vault = Vault()
        if username is None:
            username = vault.read(self.vault_path, 'username')
            password = vault.read(self.vault_path, 'password')
            client_id = vault.read(self.vault_path, 'client_id')

        url = '{}/realms/master/protocol/openid-connect/token'.format(self.url_base) # DP TODO: read realm from vault
        data = {
            'grant_type': 'password',
            'client_id': client_id,
            'username': username,
            'password': password,
        }
        response = requests.post(url, data=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        if response.status_code == 200:
            self.token = response.json()
        else:
            LOG.info("Could not authenticate to KeyCloak Server")
            self.token = None
            return None # DP TODO: should probably throw an exception, as everywhere else it is assumed that this works

        return response

    def logout(self):
        """Logout from Keycloak.

        Logout will invalidate the Keycloak session and clean the local token (
        self.token)
        """
        if self.token is None:
            return

        token_endpoint = '{}/realms/{}/protocol/openid-connect/logout'.format(self.url_base,"master") # DP TODO: read realm from vault
        data = {
            "refresh_token": self.token["refresh_token"],
            "client_id": "admin-cli",
        }
        response = requests.post(token_endpoint, data=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        self.token = None

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

    # DP TODO: figure out if always returning the json data (if it exists) works
    #          for calls like get_user_id that check the response code....

    def _get(self, url_suffix = "", params = None, headers = {}):
        url = "{}/admin/realms/{}/".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']

        response = requests.get(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        return response

    def _post(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']
        headers['Content-Type'] = 'application/json'

        response = requests.post(url, headers=headers, data=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        return response

    def _put(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']
        headers['Content-Type'] = 'application/json'

        response = requests.put(url, headers=headers, json=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        return response

    def _delete(self, url_suffix = "", data = None, headers = {}):
        url = "{}/admin/realms/{}/".format(self.url_base, self.realm, url_suffix)
        headers['Authorization'] = 'Bearer ' + self.token['access_token']

        response = requests.delete(url, headers=headers, data=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        return response

    def get_userdata(self, username):
        userid = self.get_user_id(username)
        url = "users/{}".format(userid)

        return _get(url).json()

    def get_userinfo(self):
        """Retrieve user info corresponding to the bearer_token from the 'userinfo endpoint'.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        url = 'protocol/openid-connect/userinfo'

        return _get(url).json()

    def create_user(self, user_data):
        """Create a new user in the '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        url = 'users'

        return _post(url, user_data).json()

    def reset_password(self, user_name, credentials):
        """Reset password for a user.
        Args:
           user_name (str): Username of the user
        Returns:
            requests.Response: userinfo endpoint response
        """
        user_id = self.get_user_id(user_name)
        url = 'users/{}/reset-password'.format(user_id)

        _put(url, credentials)

    def delete_user(self, user_name):
        """Delete a user from keycloak'.
        Args:
            user_name : User name of the user to be deleted
        Returns:
            requests.Response: userinfo endpoint response
        """
        userid = self.get_user_id(user_name)
        url = 'users/{}'.format(userid)

        _delete(url)

    def get_user_id(self, username):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = 'users'
        params = {
            'username': username
        }

        response = _get(url, params)
        if response.status_code == 200:
            return response.json()[0]['id']
        else:
            return None

    def get_realm_roles(self, username):
        """Retrieve all assigned realm roles from keycloak.

        Returns:
            JSON RoleRepresentation array
        """
        id = self.get_user_id(username)
        url = 'users/{}/role-mappings/realm'.format(id)

        return _get(url).json()

    def get_role_by_name(self, rolename):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = 'roles/{}'.format(rolename)

        response = _get(url)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    def map_role_to_user(self, username, rolename):
        """Map a role to user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        # Get the user id
        id = self.get_user_id(username)
        role = self.get_role_by_name(rolename)
        if id is not None and role is not None:
            url = 'users/{}/role-mappings/realm'.format(id)
            roles = json.dumps([role])

            return _post(url, roles).json()
        else:
            if id is None:
                raise Exception("Cannot locate user {}".format(username))
            else:
                raise Exception("Cannot locate role {}".format(rolename))

    def remove_role_from_user(self, username, rolename):
        """Remove a mapped role from a user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        # Get the user id
        id = self.get_user_id(username)
        role = self.get_role_by_name(rolename)
        if id is not None and role is not None:
            url = 'users/{}/role-mappings/realm'.format(id)
            roles = json.dumps([role])

            _delete(url, roles)
        else:
            if id is None:
                raise Exception("Cannot locate user {}".format(username))
            else:
                raise Exception("Cannot locate role {}".format(rolename))

    def create_group(self, group_data):
        """Create a new group if it does not exist '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        # DP ???: Why check the existence of a group and not the same for a user?
        # Check if the group exists
        group_name = json.loads(group_data)['name']
        if self.group_exists(group_name):
            print ("Group Exists") # DP TODO: Log, raise exception, ???
            return None

        url = 'groups'

        return _post(url, group_data).json()

    def delete_group(self, group_name):
        pass

    def get_all_groups(self):
        """Create a new group '.
            Args:
                bearer_token (str): the bearer token
            Returns:
                requests.Response: userinfo endpoint response
            """

        url = 'groups'

        return _get(url).json()

    def group_exists(self, group_name):
        groups = self.get_all_groups()
        for group in groups:
            if group['name'] == group_name:
                return True
        return False

    def map_group_to_user(self, username, groupname):
        pass

    def remove_group_from_user(self, username, groupname):
        pass