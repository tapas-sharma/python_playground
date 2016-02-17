#!/usr/bin/python
"""The purpose of this module is to allow all libraries to use relative
logger names. The full logger name will also include a prefix that the
application can specify.

Consider the following example: if the library uses the logger name
"lib.idb.logrotate" and the application specifies the prefix "cluster", then
the actual logger name will be "cluster.lib.idb.logrorate".

It is expected that all libraries will get their loggers using the
:py:func:`get_logger` function in this module.
"""

import os, logging
import logging.config

logging.raiseExceptions = False
CWD=os.getcwd()
_DEFAULT_LOGGING_CONFIG = CWD+'/'+"/logging.conf"

_logging_prefix = ""

def config_logging(config_file=_DEFAULT_LOGGING_CONFIG):
    open(config_file)
    logging.config.fileConfig(config_file)

def set_logging_prefix(prefix):
    """Set the logging prefix
    """
    global _logging_prefix

    _logging_prefix = prefix
    if _logging_prefix and not _logging_prefix.endswith("."):
        _logging_prefix += "."

def add_child_handler(logger, cluster_id, level=None):
    '''Function which will add handler to child logger.
    '''

    # If handlers already exists remove it first.
    if len(logger.handlers) > 0:
        h = logger.handlers[0]
        logger.removeHandler(h)
        h.close()

    # Create path og log file by considering date, time and cluster id.
    logs_path = '/logs'
    from datetime import datetime
    dateformat = datetime.now().strftime('%Y%m%d')
    final_path = '%s/%s/cid_%s' %(logs_path, dateformat, cluster_id)

    # If path we have created does not exist then create it.
    if not os.path.exists(final_path):
        os.makedirs(final_path)

    # Create filename for logging
    log_file_name = 'failover.%s.%s' % (cluster_id, datetime.now().strftime('%Y%m%d%H'))
    logger_file_path = '%s/%s' % (final_path, log_file_name)
    file_handler = logging.handlers.RotatingFileHandler(logger_file_path,
                                        'a', 10465760, 10,)
    file_handler.setFormatter(\
            logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
    file_handler.setLevel(logging.DEBUG)

    # Add handler to given logger and set its level too.
    logger.propagate = 0
    logger.setLevel(level)
  
    logger.addHandler(file_handler)

def set_formatter(logger, format):
    """Set the logging formatter
    """
    if logger.handlers and format:
        handler = logger.handlers[0]
        fmt = logging.Formatter(format)
        handler.setFormatter(fmt)

def get_logger(logger_name, relative_name=False):
    """Returns a logger; the full logger name consists of the logger_name
    argument with any previously set logging prefix prepended to it.
    """
    if relative_name:
        full_logger_name = _logging_prefix + logger_name
    else:
        full_logger_name = logger_name
    return logging.getLogger(full_logger_name)
