import logging

# Define formats for levels
FORMATS = {
    logging.DEBUG: "%(levelname)s : {Timestamp:%(asctime)s, App:%(name)s, File:%(filename)s , Line:%(lineno)d , Log_level:%(levelname)s, Message:%(message)s}",
    logging.ERROR: "%(levelname)s: {Timestamp:%(asctime)s, File:%(filename)s , Line:%(lineno)d , Log_level:%(levelname)s, Message:%(message)s}",
    logging.WARNING: "%(levelname)s: {Timestamp:%(asctime)s, File:%(filename)s , Line:%(lineno)d , Log_level:%(levelname)s, Message:%(message)s}",
    logging.CRITICAL: "%(levelname)s: {Timestamp:%(asctime)s, File:%(filename)s , Line:%(lineno)d , Log_level:%(levelname)s, Message:%(message)s}",
    logging.INFO: "%(levelname)s: {File:%(filename)s, Message:%(message)s}",
    'DEFAULT': "%(levelname)s: {Message:%(message)s}"
}
