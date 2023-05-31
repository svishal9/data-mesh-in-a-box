import datetime
import logging.config
import sys

from pytz import timezone


class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
        """Only lets through log messages with log level below ERROR ."""
        return record.levelno < logging.ERROR


def get_logging_config():
    return {
        'version': 1,
        'filters': {
            'exclude_errors': {
                '()': _ExcludeErrorsFilter
            }
        },
        'formatters': {
            # Modify log message format here or replace with your custom formatter class
            'my_formatter': {
                'format': '(%(process)d) %(asctime)s %(name)s (line %(lineno)s) | %(levelname)s %(message)s'
            }
        },
        'handlers': {
            'console_stderr': {
                # Sends log messages with log level ERROR or higher to stderr
                'class': 'logging.StreamHandler',
                'level': 'ERROR',
                'formatter': 'my_formatter',
                'stream': sys.stderr
            },
            'console_stdout': {
                # Sends log messages with log level lower than ERROR to stdout
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'my_formatter',
                'filters': ['exclude_errors'],
                'stream': sys.stdout
            },
            'file': {
                # Sends all log messages to a file
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'my_formatter',
                'filename': 'aphrodite.log',
                'encoding': 'utf8'
            }
        },
        'root': {
            # In general, this should be kept at 'NOTSET'.
            # Otherwise, it would interfere with the log levels set for each handler.
            'level': 'NOTSET',
            'handlers': ['console_stderr', 'console_stdout', 'file']
        },
    }


logging.config.dictConfig(get_logging_config())
logger = logging.getLogger('Logging from Tiger for data mesh')


def log_message(message, log_name='tiger_app_log'):
    """
    Function to log message
    :param log_name: name of log to generate
    :param message: message to log
    """
    logger.log(logging.INFO, datetime.datetime.now(timezone('Australia/Sydney')).strftime(
        "%Y-%m-%d %H:%M:%S") + f" - {log_name} : {message}")
