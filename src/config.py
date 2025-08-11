import logging.config
import os
import sys

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

def configure_logging(debug_mode=False):
    """Configures logging dynamically using dictConfig with separate log files."""
    log_level = logging.DEBUG if debug_mode else logging.INFO

    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(levelname)s - %(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "simple",
                "stream": sys.stdout,
            },
            "application_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "application.log",
                "mode": "a",
            },
            "api_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "api.log",
                "mode": "a",
            },
            "analytics_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "analytics.log",
                "mode": "a",
            },
            "data_access_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "data_access.log",
                "mode": "a",
            },
            "processing_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "processing.log",
                "mode": "a",
            },
        },
        "loggers": {
            "application": {
                "level": log_level,
                "handlers": ["application_file"],
                "propagate": False,
            },
            "analytics": {
                "level": log_level,
                "handlers": ["analytics_file"],
                "propagate": False,
            },
            "api": {
                "level": log_level,
                "handlers": ["api_file"],
                "propagate": False,
            },
            "data_access": {
                "level": log_level,
                "handlers": ["data_access_file"],
                "propagate": False,
            },
            "processing": {
                "level": log_level,
                "handlers": ["processing_file"],
                "propagate": False,
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "application_file"],
        },
    }

    logging.config.dictConfig(log_config)

# if __name__ == "__main__":
    # configure_logging()
