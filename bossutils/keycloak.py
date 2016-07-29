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

class KeyCloakClient:
    """Client for connecting to Keycloak and using the REST API.

    Client provides a method for issuing requests to the Keycloak REST API and
    a set of methods to simplify Keycloak configuration.
    """
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

    def login(self, client_id, username=None, password=None):
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
            username = vault.read('secret/endpoint/auth', 'username')
            password = vault.read('secret/endpoint/auth', 'password')

        url = '{}/realms/master/protocol/openid-connect/token'.format(self.url_base)
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
            print("Could not authenticate to KeyCloak Server")
            return None 

        return response

    def logout(self):
        """Logout from Keycloak.

        Logout will invalidate the Keycloak session and clean the local token (
        self.token)
        """
        if self.token is None:
            return
        token_endpoint = '{}/realms/{}/protocol/openid-connect/logout'.format(self.url_base,self.realm)
        data = {
            "refresh_token": self.token["refresh_token"],
            "client_id": "admin-cli",
        }
        response = requests.post(token_endpoint, data=data, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        self.token = None

    def get_userinfo(self, bearer_token=None):
        """Retrieve user info corresponding to the bearer_token from the 'userinfo endpoint'.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """

        if bearer_token is None:
            bearer_token= self.token['access_token']
        url = '{}/realms/{}/protocol/openid-connect/userinfo'.format(self.url_base, self.realm)
        headers = {
            'Authorization': 'Bearer ' + bearer_token,
        }
        response = requests.get(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response

    def create_user(self, user_data, bearer_token=None):
        """Create a new user in the '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        if bearer_token is None:
            bearer_token = self.token['access_token']
        url = '{}/admin/realms/{}/users'.format(self.url_base, self.realm)
        #user_data = json.loads(user_data)
        #print (type(user_data))
        #user_data["enabled"]=1
        #print (user_data)

        headers = {
            'Authorization': 'Bearer ' + bearer_token
,
            'Content-Type': 'application/json'
        }
        response = requests.post(url, data=user_data, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response

    def delete_user(self, user_name, realm, bearer_token=None):
        """Delete a user from keycloak'.
        Args:
            user_name : User name of the user to be deleted
        Returns:
            requests.Response: userinfo endpoint response
        """
        if bearer_token is None:
            bearer_token = self.token['access_token']

        userid = self.get_user_id(user_name, realm)

        url = '{}/admin/realms/{}/users/{}'.format(self.url_base, realm, userid)

        headers = {
            'Authorization': 'Bearer ' + bearer_token,
            'Content-Type': 'application/json'
        }
        response = requests.delete(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response

    def get_user_id(self, username, realm):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = '{}/admin/realms/{}/users'.format(self.url_base, realm)
        params = {
            'username': username
        }
        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
        }
        response = requests.get(url, headers=headers, params=params, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        if response.status_code == 200:
            user = response.json()[0]
            return user['id']
        else:
            return None

    def get_all_realms(self):
        """Retrieve all avilable realm roles from keycloak.

            Returns:
                requests.Response: userinfo endpoint response
            """
        url = '{}/admin/realms/'.format(self.url_base)
        print(url)
        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
        }
        response = requests.get(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response

    def get_realm_roles(self, username, realm):
        """Retrieve all avilable realm roles from keycloak.

            Returns:
                requests.Response: userinfo endpoint response
            """
        id = self.get_user_id(username, realm)
        url = '{}/admin/realms/{}/users/{}/role-mappings/realm/available'.format(self.url_base, realm,id)
        print (url)
        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
        }
        response = requests.get(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response



    def get_role_by_name(self, rolename, realm):
        """Retrieve the user id for the given user.

        Returns:
        requests.Response: userinfo endpoint response
        """
        url = '{}/admin/realms/{}/roles/{}'.format(self.url_base, realm,rolename)

        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
        }
        response = requests.get(url, headers=headers, verify=self.https and self.verify_ssl)
        response.raise_for_status()

        if response.status_code == 200:
            return response.json()
        else:
            return None

    def map_role_to_user(self, username, rolename, realm):
        """Map a role to user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        # Get the user id

        id = self.get_user_id(username,realm)
        role = self.get_role_by_name(rolename,realm)
        roles = []
        roles.append(role)
        roles = json.dumps(roles)
        if id:
            url = '{}/admin/realms/{}/users/{}/role-mappings/realm'.format(self.url_base, realm, id)
            print (url)
            headers = {
                'Authorization': 'Bearer ' + self.token['access_token'],
                'Content-Type': 'application/json'
            }
            response = requests.post(url, data=roles, headers=headers,verify=self.https and self.verify_ssl)
            response.raise_for_status()
            return response
        else:
            print ("Cannot verify user")
            return None

    def remove_role_from_user(self, username, rolename, realm):
        """Remove a mapped role from a user in a specified realm '.
        Args:
            bearer_token (str): the bearer token
        Returns:
            requests.Response: userinfo endpoint response
        """
        # Get the user id

        id = self.get_user_id(username, realm)
        role = self.get_role_by_name(rolename, realm)
        roles = []
        roles.append(role)
        roles = json.dumps(roles)
        if id:
            url = '{}/admin/realms/{}/users/{}/role-mappings/realm'.format(self.url_base, realm, id)
            print(url)
            headers = {
                'Authorization': 'Bearer ' + self.token['access_token'],
                'Content-Type': 'application/json'
            }
            response = requests.delete(url, data=roles, headers=headers, verify=self.https and self.verify_ssl)
            response.raise_for_status()
            return response
        else:
            print("Cannot verify user")
            return None

    def create_group(self, group_data, realm):
        """Create a new group if it does not exist '.
            Args:
                bearer_token (str): the bearer token
            Returns:
                requests.Response: userinfo endpoint response
            """

        # Check if the group exists
        group_name = json.loads(group_data)['name']
        if self.group_exists(realm, group_name):
            print ("Group Exists")
            return None

        url = '{}/admin/realms/{}/groups'.format(self.url_base, realm)

        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
            'Content-Type': 'application/json'
        }
        response = requests.post(url, data=group_data, headers=headers,
                                 verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response

    def get_all_groups(self, realm):
        """Create a new group '.
            Args:
                bearer_token (str): the bearer token
            Returns:
                requests.Response: userinfo endpoint response
            """

        url = '{}/admin/realms/{}/groups'.format(self.url_base, realm)

        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers,verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response.json()

    def group_exists(self, realm, group_name):

        groups = self.get_all_groups(realm)
        print (type(groups))
        for group in groups:
            print (group['name'])
            if group['name'] == group_name:
                return True
        return False

    def create_realm(self, realm):
        """Create a new realm based on the JSON based configuration.

            Note: User must be logged into Keycloak first

        Args:
            realm (dict) : JSON dictory configuration for the new realm
        """
        #
        url = '{}/admin/realms/'.format(self.url_base)

        headers = {
            'Authorization': 'Bearer ' + self.token['access_token'],
            'Content-Type': 'application/json'
        }
        response = requests.post(url, data=json.dumps(realm), headers=headers,
                                 verify=self.https and self.verify_ssl)
        response.raise_for_status()
        return response


if __name__ == '__main__':

    #kc = KeyCloakClient('master', 'https://auth.manavpj1.theboss.io/auth')
    kc = KeyCloakClient('master')
    #    token = kc.login('admin-cli','admin','9AeofIfpZfeuqs90').json()
    token = kc.login('admin-cli',).json()
    print(token['access_token'])
    print(kc.get_userinfo().json())
    kc.logout()



