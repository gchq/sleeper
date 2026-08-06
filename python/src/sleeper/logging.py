# sleeper/logging.py

import logging


def enable_logging(level: int = logging.INFO) -> None:
    logger = logging.getLogger("sleeper")

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(filename)s/%(funcName)s %(message)s"))

    logger.addHandler(handler)
    logger.setLevel(level)
