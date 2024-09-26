import logging
from logging import Logger
import os
from typing import Literal
from utils import ROOT_DIR


def setup_logging(logger_name: str) -> Logger:
    """Sets up a logger for the calling script.

    Parameters
    ----------
    logger_name: str
        The name of the log file.

    Returns
    -------
    Logger
    """
    logger = logging.getLogger(logger_name)
    handler = logging.FileHandler(os.path.join(ROOT_DIR, "logs", logger_name))
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def log_msg(
    logger: Logger,
    msg: str,
    level: Literal["info", "warning", "error"] = "info",
    to_stdout: bool = False,
) -> None:
    """Logs and optionally prints a message.

    Parameters
    ----------
    logger: Logger
        The logger to use.
    msg: str
        The message to log.
    level: Literal["info", "warning", "error"], optional
        The log level, defaults to "info".
    to_stdout: bool, optional
        Whether to print the message as well, defaults to False.
    """
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    if to_stdout:
        print(msg)
