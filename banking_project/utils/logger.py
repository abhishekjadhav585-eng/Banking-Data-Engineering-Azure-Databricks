import logging
import sys


def get_logger(name):
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger