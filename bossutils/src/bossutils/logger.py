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

import logging
import logging.config
import json
from pkg_resources import resource_filename

def configure(json_config_file=None):
    """Configure the logging module for use by Boss code

    Args:
        json_config_file (optional[str]): Path to json file defining logging configuration
    """
    if not json_config_file:
        json_config_file = resource_filename('bossutils', 'logger_conf.json')

    with open(json_config_file) as f:
        cfgDict = json.load(f)
    logging.config.dictConfig(cfgDict)

def bossLogger():
    """Get the logger for Boss code

    Returns:
        logging.Logger
    """
    return logging.getLogger('boss')

def lambdaLogger(level=logging.INFO):
    """Get the logger for Boss code running in AWS Lambda

    Returns:
        logging.Logger
    """
    log = logging.getLogger()
    log.setLevel(level)
    return log
