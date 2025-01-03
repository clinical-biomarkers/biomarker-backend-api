""" Handles the backend logic for the API logging.
"""

from flask import Request, current_app, g
from user_agents import parse
from . import FRONTEND_CALL_LOG_TABLE, utils as utils
from . import LOG_DB_PATH, API_CALL_LOG_TABLE
from .db import create_timestamp, cast_app
from typing import Optional, Dict, Tuple
import json
import traceback
import sqlite3


def get_api_log_db():
    if "log_db" not in g:
        g.log_db = sqlite3.connect(LOG_DB_PATH)
    return g.log_db


def frontend_log(api_request: Request) -> Tuple[Dict, int]:
    """Entry point for the frontend logging endpoint.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return object and HTTP code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "frontend_logging"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    _log_frontend_action(request_arguments)

    return {"status": "success"}, 200


def api_log(
    request_object: Optional[Dict],
    endpoint: str,
    api_request: Request,
    duration: float,
    status_code: int,
):
    """Logs an API request in the api_calls table.

    Parameters
    ----------
    request_object : dict or None
        The parsed query string parameters associated with the API call (if applicable).
    endpoint : str
        The endpoint the request came in for.
    api_request : Request
        The flask request object.
    duration : float
        The duration of the request processing in seconds.
    status_code : int
        The HTTP statuc code of the response.
    """

    user_agent = parse(api_request.headers.get("User-Agent"))
    is_bot = user_agent.is_bot
    timestamp = create_timestamp()

    log_entry = {
        "timestamp": timestamp,
        "date": timestamp.split(" ")[0],
        "endpoint": endpoint,
        "request": json.dumps(request_object),
        "user_agent": str(user_agent),
        "referer": api_request.headers.get("Referer"),
        "origin": api_request.headers.get("Origin"),
        "is_bot": str(is_bot),
        "ip": api_request.environ.get("HTTP_X_FORWARDED_FOR", api_request.remote_addr),
        "duration": duration,
        "status_code": status_code,
    }

    custom_app = cast_app(current_app)

    try:
        db = get_api_log_db()
        cursor = db.cursor()

        columns = ", ".join(log_entry.keys())
        placeholders = ", ".join("?" * len(log_entry))

        sql = f"INSERT INTO {API_CALL_LOG_TABLE} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(log_entry.values()))
        db.commit()

    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to log API call: {str(e)}\n{traceback.format_exc}"
        )


def _log_frontend_action(request_object: Dict):
    """Logs an API request in the frontend call log table.

    Parameters
    ----------
    request_object : dict
        The validated request object from the user API call.
    """
    timestamp = create_timestamp()

    log_entry = {
        "call_id": request_object["id"],
        "timestamp": timestamp,
        "date": timestamp.split(" ")[0],
        "user": request_object["user"],
        "type": request_object["type"],
        "page": request_object["page"],
        "message": request_object["message"],
    }

    custom_app = cast_app(current_app)

    try:
        db = get_api_log_db()
        cursor = db.cursor()

        columns = ", ".join(log_entry.keys())
        placeholders = ", ".join("?" * len(log_entry))

        sql = (
            f"INSERT INTO {FRONTEND_CALL_LOG_TABLE} ({columns}) VALUES ({placeholders})"
        )
        cursor.execute(sql, list(log_entry.values()))
        db.commit()

    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to log frontend action: {str(e)}\n{traceback.format_exc}"
        )
