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

# __init__


import logging.config
import json
from pkg_resources import resource_filename

from .formats import *


class BossLogger:
    """Custom logger for Boss.

    Attributes:
        logger (Logger): A configured python logging.logger instance

    Example usage:
        log = BossLogger().logger
        logger.info('my log msg')
    """


    def __init__(self, json_config_file=None):
        """Constructor.

        Args:
            json_config_file (optional[string]): A JSON config file to configure the logger.  Defaults to resource_filename('bossutils', 'logger_conf.json').

        Returns:
            (BossLogger)
        """
        if not json_config_file:
            json_config_file = resource_filename('bossutils', 'logger_conf.json')

        with open(json_config_file) as f:
            cfgDict = json.load(f)
        logging.config.dictConfig(cfgDict)

        self.logger = logging.getLogger('boss')

    def setLevel(self, level):
        """
        Set the level of the root logger
        :param level:  String representing the desired debug level
        :return:
        """
        if level.lower() == "error":
            self.logger.setLevel(logging.ERROR)
        elif level.lower() == "warning":
            self.logger.setLevel(logging.WARNING)
        elif level.lower() == "debug":
            self.logger.setLevel(logging.DEBUG)
        elif level.lower() == "info":
            self.logger.setLevel(logging.INFO)
        elif level.lower() == "critical":
            self.logger.setLevel(logging.CRITICAL)


def bossFormatterFactory():
    """Create a BossFormatter with formats defined in .formats.

    This factory allows use of BossFormatter from config files.
    """
    return BossFormatter(FORMATS)


class BossFormatter(logging.Formatter):
    """ A custom formatter that defined a format for each log level. """
    default_formatter = logging.Formatter('%(levelname)s: Message: %(message)s')

    def __init__(self, formats):
        """ Initialize the formatter class
        :param formats: dict { loglevel : logformat }
        """
        self.formatters = {}
        for loglevel in formats:
            self.formatters[loglevel] = logging.Formatter(formats[loglevel])

    def format(self, record):

        """
        Format the log record based using the formats specified in BossFormatter
        :param record: Log record to be formatted
        :return: Formatted record
        """
        formatter = self.formatters.get(record.levelno, self.default_formatter)
        return formatter.format(record)
