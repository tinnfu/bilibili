# coding: utf-8
from logging import StreamHandler, FileHandler, Formatter, getLogger, INFO, DEBUG, WARN
import time
import os
import sys

def get_logger(log_name = 'default.log', log_level = DEBUG):
    formatter = Formatter('[%(asctime)s][%(process)d][%(filename)s:%(lineno)s][%(levelname)s]: %(message)s')
    logger = getLogger('%s.%s' % (log_name.replace('/', '.'),
                                                time.time()))
    logger.handlers = []
    logger.setLevel(DEBUG)
    logger.propagate = False

    console = StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(WARN)
    logger.addHandler(console)

    logfile = os.path.dirname(os.path.abspath(sys.argv[0]))
    logfile = os.path.join(logfile, log_name)
    logfiledebug = FileHandler(filename = logfile, mode='a')
    logfiledebug.setFormatter(formatter)
    logfiledebug.setLevel(log_level)
    logger.addHandler(logfiledebug)

    def _logger_die(logger, msg):
        logger.error(msg)
        raise AssertionError(msg)

    logger.die = lambda msg: _logger_die(logger, msg)

    return logger
