import logging
from logging import Logger
import os
from typing import Literal
from tutils import ROOT_DIR


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
    log_dir_path = os.path.join(ROOT_DIR, "logs")
    if not os.path.isdir(log_dir_path):
        os.mkdir(log_dir_path)
    handler = logging.FileHandler(filename=os.path.join(ROOT_DIR, "logs", logger_name))
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


def start_message(logger: Logger, msg: str, to_stdout: bool = True) -> None:
    """Logs a standardized start message."""
    start_message = f"############### {msg} ###############"
    log_msg(logger=logger, msg=start_message, to_stdout=to_stdout)


def elapsed_time_formatter(seconds: float, round_digits: int = 2) -> str:
    seconds = round(seconds, round_digits)
    minutes = round(seconds / 60, round_digits)
    hours = round(minutes / 60, round_digits)
    return f"{hours} hours ({minutes} minutes/{seconds} seconds)"
