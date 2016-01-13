import hvac
from . import configuration

VAULT_SECTION = "vault"
VAULT_URL_KEY = "url"
VAULT_TOKEN_KEY = "token"

class Vault:
    def __init__(self, config = None):
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
        self.client.logout()
        self.client = None
        
    def rotate_token(self):
        new_token = self.read("/cubbyhole", "token")
        self.client.token = new_token
        
        if not self.client.is_authenticated():
            raise Exception("Could not authenticate with rotated Vault token")
        
        self.config[VAULT_SECTION][VAULT_TOKEN_KEY] = new_token
        with open(configuration.CONFIG_FILE, "w") as fh:
            self.config.write(fh)
    
    def read_dict(self, path):
        response = self.client.read(path)
        
        if response is None:
            raise Exception("Could not locate {} in Vault".format(path))
            
        return response["data"]
    
    def read(self, path, key):
        response = self.client.read(path)
        if response is not None:
            response = response["data"][key]
            
        if response is None:
            raise Exception("Could not locate {}/{} in Vault".format(path,key))
            
        return response
        
    def write(self, path, **kwargs):
        self.client.write(path, **kwargs)
        
    def delete(self, path):
        self.client.delete(path)