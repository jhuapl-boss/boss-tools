"""A class for interacting with the Vault server.

VAULT_SECTION is the config section header for Vault information
VAULT_URL_KEY is the config key for the Vault url
VAULT_TOKEN_KEY is the config key for the Vault access token
"""

import hvac
from . import configuration

VAULT_SECTION = "vault"
VAULT_URL_KEY = "url"
VAULT_TOKEN_KEY = "token"

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

        self.client = hvac.Client(url=url, token=token)

        if not self.client.is_authenticated():
            raise Exception("Could not authenticate to Vault server")

    def logout(self):
        """Logout and clear the internal state.

        The object will no longer work after this call."""
        self.client.logout()
        self.client = None

    def rotate_token(self):
        """Read a new token from the current cubbyhold and override the token
        stored in the BossConfig."""
        new_token = self.read("/cubbyhole", "token")
        self.client.token = new_token

        if not self.client.is_authenticated():
            raise Exception("Could not authenticate with rotated Vault token")

        self.config[VAULT_SECTION][VAULT_TOKEN_KEY] = new_token
        with open(configuration.CONFIG_FILE, "w") as fh:
            self.config.write(fh)

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

    def read(self, path, key):
        """Read the specific key from the Vault path.

        An Exception is thrown is the path or key do not exist."""
        response = self.client.read(path)
        if response is not None:
            response = response["data"][key]

        if response is None:
            raise Exception("Could not locate {}/{} in Vault".format(path,key))

        return response

    def write(self, path, **kwargs):
        """Write the given key / values in the given Vault path."""
        self.client.write(path, **kwargs)

    def delete(self, path):
        """Delete the given Vault path."""
        self.client.delete(path)

    def revoke_secret(self, lease_id):
        """Revoke the given Vault secret."""
        self.client.revoke_secret(lease_id)

    def renew_secret(self, lease_id):
        """Renew the given Vault secret."""
        return self.client.renew_secret(lease_id)