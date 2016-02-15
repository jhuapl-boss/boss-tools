# __init__


import logging.config
from pkg_resources import resource_filename

from .formats import *

LOG_FILE = "/var/log/boss/boss.log"


class BossLogger:
    def __init__(self):

        config_file = resource_filename('bossutils', 'logger.conf')
        logging.config.fileConfig(config_file)
        self.logger = logging.getLogger('boss')

        # Add a default handler to the logger
        fh1 = logging.FileHandler(LOG_FILE)

        # Set the default logger level
        fh1.setLevel(logging.INFO)
        formatter = BossFormatter(FORMATS)
        fh1.setFormatter(formatter)
        self.logger.addHandler(fh1)

    def info(self, msg):
        """
        Log messages with log level info
        :param msg: Log Message
        :return:
        """
        self.logger.info(msg)

    def debug(self, msg):
        """
        Log messages with log level debug
        :param msg: Log Message
        :return:
        """
        self.logger.debug(msg)

    def warning(self, msg):
        """
        Log messages with log level warning
        :param msg: Log Message
        :return:
        """
        self.logger.warning(msg)

    def error(self, msg):
        """
        Log messages with log level error
        :param msg: Log Message
        :return:
        """
        self.logger.error(msg)

    def critical(self, msg):
        """
        Log messages with log level critical
        :param msg: Log Message
        :return:
        """
        self.logger.critical(msg)

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
