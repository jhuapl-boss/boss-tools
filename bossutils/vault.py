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

"""A class for interacting with the Vault server.

VAULT_SECTION is the config section header for Vault information
VAULT_URL_KEY is the config key for the Vault url
VAULT_TOKEN_KEY is the config key for the Vault access token
"""

import hvac
from . import configuration
from . import utils
import functools
from bossutils.logger import BossLogger

VAULT_SECTION = "vault"
VAULT_URL_KEY = "url"
VAULT_TOKEN_KEY = "token"

def catch_expire(function):
    """Catches error raised when token has expired and rotates token if so.
    Executes the same function once the token has been replaced."""
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Forbidden as e:
            blog = BossLogger().logger
            blog.info(str(e))
            msg = "Your token had expired.  Dynamically creating a new one."
            blog.info(msg)
            Vault.login(self=None)
            function(*args, **kwargs)
    return wrapper

class Vault:
    """Vault is a wrapper class for the hvac Vault client that automatically
    loads the Vault server url and access token from the BossConfig and
    verifies that the access token can authenticate.
    """
    def __init__(self, config = None):
        """Load the Vault information from BossConfig() and verify
        the authentication status. If the token is not valid an Exception
        is raised."""
        if config is None:
            self.config = configuration.BossConfig()
        else:
            self.config = config

        url = self.config[VAULT_SECTION][VAULT_URL_KEY]
        token = self.config[VAULT_SECTION][VAULT_TOKEN_KEY]

        self.client = hvac.Client(url=url)
        if token == "":
            self.login()
        else:
            self.client.token = token
            if not self.client.is_authenticated():
                self.login()

    def logout(self):
        """Logout and clear the internal state.

        The object will no longer work after this call."""
        self.client.logout()
        self.client = None

    def login(self):
        pkcs7 = utils.read_url(utils.DYNAMIC_URL + 'instance-identity/pkcs7').replace('\n', '')
        role = self.config['system']['type']

        # DP NOTE: Current version of hvac client (0.2.15) does not contain the auth_ec2 method
        #response = self.client.auth_ec2(pkcs7, role = role)
        data = {'pkcs7': pkcs7, 'role': role, 'nonce': 'BOSS Vault Client'}
        response = self.client.auth('/v1/auth/aws-ec2/login', json = data)

        if not self.client.is_authenticated():
            raise Exception("Could not authenticate with ec2 Vault token")

        self.config[VAULT_SECTION][VAULT_TOKEN_KEY] = response['auth']['client_token']
        with open(configuration.CONFIG_FILE, "w") as fh:
            self.config.config.write(fh)

    def read_dict(self, path, raw=False):
        """Read a dictionary of keys from the Vault path.

        raw is if the raw Vault response should be returned, or just the data

        An Exception is thrown is the path does not exist."""
        response = self.client.read(path)

        if response is None:
            raise Exception("Could not locate {} in Vault".format(path))

        if raw:
            return response
        else:
            return response["data"]

    @catch_expire
    def read(self, path, key):
        """Read the specific key from the Vault path.

        An Exception is thrown is the path or key do not exist."""
        response = self.client.read(path)
        if response is not None:
            response = response["data"][key]

        if response is None:
            raise Exception("Could not locate {}/{} in Vault".format(path,key))

        return response
    
    @catch_expire
    def write(self, path, **kwargs):
        """Write the given key / values in the given Vault path."""
        self.client.write(path, **kwargs)

    @catch_expire
    def delete(self, path):
        """Delete the given Vault path."""
        self.client.delete(path)

    @catch_expire
    def revoke_secret(self, lease_id):
        """Revoke the given Vault secret."""
        self.client.revoke_secret(lease_id)

    @catch_expire
    def revoke_secret_prefix(self, prefix):
        """Revoke Vault secret(s) starting with the given prefix.

        Args:
            prefix (string): Prefix that the vault secret begins with.
        """
        self.client.revoke_secret_prefix(prefix)

    @catch_expire
    def renew_secret(self, lease_id):
        """Renew the given Vault secret."""
        return self.client.renew_secret(lease_id)
