# -*- coding: utf-8 -*-

import logging
import logging.config  # Has to be imported separately


def setup(log_level, log_file_name):
    """Configure the logging system."""
    # Log format string for flake8 compliance
    log_fmt = "%(levelname)-8s  %(asctime)s " "%(filename)s:%(lineno)-4s %(message)s"

    dict_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": log_fmt, "datefmt": "%Y-%m-%d %H:%M:%S"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "default"},
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": log_file_name,
                "backupCount": 2,
                "maxBytes": 10 * 1024,
                "delay": True,  # Ony write to file on first byte emitted.
            },
        },
        "loggers": {
            "duplicates": {"level": log_level, "handlers": ["console", "file"]},
            "accruals": {"level": log_level, "handlers": ["console", "file"]},
        },
    }

    logging.config.dictConfig(dict_config)


def set_log_level(log_level, quiet, verbose):
    log_levels = {2: "ERROR", 1: "WARNING", 0: "INFO", -1: "DEBUG"}
    if log_level is None:
        level = quiet - verbose
        level = max(level, -1)  # No smaller than -1
        level = min(level, 2)  # No larger than 2
        return log_levels[level]
    return log_level
