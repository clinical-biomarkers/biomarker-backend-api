""" General functions that interact with the general database collections such as the
logging collection and the error logging collection.
"""

from flask import current_app, Request, Flask
from . import (
    REQ_LOG_COLLECTION,
    REQ_LOG_MAX_LEN,
    ERROR_LOG_COLLECTION,
    TIMESTAMP_FORMAT,
    TIMEZONE,
    DB_COLLECTION,
    SEARCH_CACHE_COLLECTION,
)
from typing import Optional, Dict, cast, Tuple, Union, List
import datetime
import pytz  # type: ignore
import string
import random
import json
from user_agents import parse  # type: ignore
from ...biomarker import CustomFlask  # type: ignore
from pymongo.errors import PyMongoError


def log_request(
    request_object: Optional[Dict], endpoint: str, api_request: Request
) -> Tuple[int, Optional[str]]:
    """Logs an API request in the request log collection.

    Parameters
    ----------
    request_object : dict or None
        The parsed query string parameters associated with the API call (if available).
    endpoint : str
        The endpoint the request came in for.
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (int, str or None)
        The exit status; 0 for success, 1 for failure to log. If failure, also
        returns the error ID.
    """
    if request_object and len(json.dumps(request_object)) > REQ_LOG_MAX_LEN:
        _, error_id = log_error(
            error_msg=f"Request object length exceeds REQ_LOG_MAX_LEN ({REQ_LOG_MAX_LEN})",
            origin="log_request",
        )
        return (1, error_id)

    header_dict = {
        "user_agent": api_request.headers.get("User-Agent"),
        "referer": api_request.headers.get("Referer"),
        "origin": api_request.headers.get("Origin"),
        "ip": api_request.environ.get("HTTP_X_FORWARDED_FOR", api_request.remote_addr),
    }
    user_agent = parse(api_request.headers.get("User-Agent"))
    header_dict["is_bot"] = user_agent.is_bot

    log_object = {
        "api": endpoint,
        "request": request_object,
        "timestamp": create_timestamp(),
        "headers": header_dict,
    }
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    try:
        dbh[REQ_LOG_COLLECTION].insert_one(log_object)
        return (0, None)
    except Exception as e:
        _, error_id = log_error(
            error_msg=f"Failed to log request.\n{e}", origin="log_request"
        )
        return (1, error_id)


def log_error(error_msg: str, origin: str) -> Tuple[int, Optional[str]]:
    """Logs an error in the error collection log.

    Parameters
    ----------
    error_msg : str
        The error message to log (a traceback stack trace or custom
        error message).
    origin : str
        The function calling this function.

    Returns
    -------
    tuple : (int, str or None)
        The exit status; 0 for success, 1 for failure to log. If success, also
        returns the error ID.
    """

    def _create_error_id(
        size: int = 6, chars: str = string.ascii_uppercase + string.digits
    ) -> str:
        """Creates an error ID.

        Parameters
        ----------
        size : int (default: 6)
            The error ID string length to generate.
        chars : str (default: string.ascii_uppercase + string.digits)
            The character set to sample from.

        Returns
        -------
        str
            The random error ID.
        """
        return "".join(random.choice(chars) for _ in range(size))

    error_id = _create_error_id()
    error_object = {
        "id": error_id,
        "msg": error_msg,
        "origin": origin,
        "timestamp": create_timestamp(),
    }
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db
    try:
        dbh[ERROR_LOG_COLLECTION].insert_one(error_object)
        return (0, error_id)
    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to log error.\n{e}\nError object: {error_object}"
        )
        return (1, None)


def find_one(
    query_object: Dict,
    projection_object: Dict = {"_id": 0},
    collection: str = DB_COLLECTION,
) -> Tuple[int, Union[Dict, str, None]]:
    """Performs a find_one query on the specified collection.

    Parameters
    ----------
    query_object : dict
        The MongoDB query object.
    projection_object : dict (default: {"_id": 0})
        The projection object, by default it returns everything
        but the internal MongoDB _id field.
    collection : str (default: DB_COLLECTION)
        The collection to search on.

    Returns
    -------
    tuple : (int, dict str or None)
        The status code and the found document or None if no document was found
        or an error occurred. Or the error ID on non-success exit code. 0 for
        successful query (dict can still be None if no document was found), 1 for
        caught exception.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db
    try:
        result = dbh[collection].find_one(query_object, projection_object)
    except PyMongoError as db_error:
        _, error_id = log_error(
            error_msg=f"PyMongoError querying database during find_one.\n{db_error}",
            origin="find_one",
        )
        return 1, error_id
    except Exception as e:
        _, error_id = log_error(
            error_msg=f"Non-PyMongoError querying database during find_one.\n{e}",
            origin="find_one",
        )
        return 1, error_id
    return 0, result


def create_timestamp() -> str:
    """Creates a current timestamp.

    Returns
    -------
    str
        The current timestamp as a string.
    """
    timestamp = datetime.datetime.now(pytz.timezone(TIMEZONE)).strftime(
        TIMESTAMP_FORMAT
    )
    return timestamp


def cast_app(app: Flask) -> CustomFlask:
    """Casts the Flask app as the CustomFlask instance for
    static type checkers.

    Parameters
    ----------
    app : Flask
        The Flask current_app instance.

    Returns
    -------
    CustomFlask
        The casted current_app instance.
    """
    custom_app = cast(CustomFlask, app)
    return custom_app
