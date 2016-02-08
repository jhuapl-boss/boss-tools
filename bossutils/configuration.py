"""Functions and Classes for interacting with /etc/boss/boss.config

CONFIG_FILE is the location of the boss.config file
"""

import configparser
from . import utils

CONFIG_FILE = "/etc/boss/boss.config"

def download_and_save():
    """Download the boss.config file from User-data and save it to CONFIG_FILE"""
    user_data = utils.read_url(utils.USERDATA_URL)

    with open(CONFIG_FILE, "w") as fh:
        fh.write(user_data)

class BossConfig:
    """BossConfig is a wrapper around ConfigParser() that automatically loads the
    config file from CONFIG_FILE."""
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE)
        
    def __getitem__(self, key):
        return self.config[key]
