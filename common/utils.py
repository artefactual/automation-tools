from six.moves import configparser
import logging
import os
import requests


def get_setting(config_file, config_name, setting, default=None):
    """
    Get setting value

    :param str config_file: Configuration file path
    :param str config_name: Name of configuration
    :param str setting: Name of configuration setting to look up
    :param str default: Default value if no configuration setting exists
    :returns: str Configuration value
    """
    config = configparser.SafeConfigParser()
    try:
        config.read(config_file)
        return config.get(config_name, setting)
    except Exception:
        return default


def configure_logging(name, filename, loglevel):
    """
    Configure logging

    :param str name: Name of logger
    :param str filename: Filename of log
    :returns: None
    """
    CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(levelname)-8s  %(asctime)s  %(filename)s:%(lineno)-4s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': filename,
                'backupCount': 2,
                'maxBytes': 10 * 1024,
            },
        },
        'loggers': {
            name: {
                'level': loglevel,  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
                'handlers': ['console', 'file'],
            },
        },
    }
    logging.config.dictConfig(CONFIG)


def open_pid_file(pid_file, logger=None):
    """
    Open pidfile

    :param str pid_file: Desired path for pidfile
    :param Logger logger: Logger to log opening issues to
    :returns: True if a pidfile could be opened or None
    """
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), 'r+')
    except OSError:
        if logger:
            logger.info('Error accessing pid file %s:', pid_file, exc_info=True)
        return None
    except Exception:
        if logger:
            logger.info('This script is already running. To override this behaviour and start a new run, remove %s', pid_file)
        return None
    else:
        pid = os.getpid()
        f.write(str(pid))
        f.close()
    return True


def call_url_json(url, params, logger=None):
    """
    Helper to GET a URL where the expected response is 200 with JSON.

    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or None
    """
    if logger:
        logger.debug('URL: %s; params: %s;', url, params)
    response = requests.get(url, params=params)
    if logger:
        logger.debug('Response: %s', response)
    if not response.ok:
        if logger:
            logger.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
            logger.debug('Response: %s', response.text)
        return None
    try:
        return response.json()
    except ValueError:  # JSON could not be decoded
        if logger:
            logger.warning('Could not parse JSON from response: %s', response.text)
        return None
