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

"""Functions and Classes for interacting with /etc/boss/boss.config

CONFIG_FILE is the location of the boss.config file
"""

import configparser
from . import utils

CONFIG_FILE = "/etc/boss/boss.config"

def download_and_save():
    """Download the boss.config file from User-data and save it to CONFIG_FILE"""
    user_data = utils.read_url(utils.USERDATA_URL)

    if not user_data.strip().startswith('['):
        raise Exception("User Data is not an INI file")

    with open(CONFIG_FILE, "w") as fh:
        fh.write(user_data)

class BossConfig:
    """BossConfig is a wrapper around ConfigParser() that automatically loads the
    config file from CONFIG_FILE."""
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.optionxform = str  # this line perserves the case of the keys.
        self.config.read(CONFIG_FILE)
        
    def __getitem__(self, key):
        return self.config[key]
