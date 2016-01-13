import configparser
from . import utils

CONFIG_FILE = "/etc/boss/boss.config"

def download_and_save():
    user_data = utils.read_url(utils.USERDATA_URL)

    with open(CONFIG_FILE, "w") as fh:
        fh.write(user_data)

class BossConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE)
        
    def __getitem__(self, key):
        return self.config[key]
