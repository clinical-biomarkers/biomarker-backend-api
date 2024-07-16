""" Handles the backend logic for the API logging.
"""

from flask import Request, current_app
from user_agents import parse  # type: ignore
from . import API_LOG_DICT, FRONTEND_LOG_DICT
from . import utils as utils
from .db import create_timestamp, cast_app
from typing import Optional, Dict, Tuple
from uuid import uuid4
import json
import traceback


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

    log_frontend_action(request_arguments)

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
        if API_LOG_DICT is not None:
            API_LOG_DICT[str(uuid4())] = log_entry
        else:
            custom_app.api_logger.error(
                "API_LOG_DICT not initialized, cannot log API call."
            )
    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to log API call: {str(e)}\n{traceback.format_exc}"
        )


def log_frontend_action(request_object: Dict):
    """Logs an API request in the frontend call log table.

    Parameters
    ----------
    request_object : dict
        The validated request object from the user API call.
    """
    timestamp = create_timestamp()

    log_entry = {
        "id": request_object["id"],
        "timestamp": timestamp,
        "date": timestamp.split(" ")[0],
        "user": request_object["user"],
        "type": request_object["type"],
        "page": request_object["page"],
        "message": request_object["message"],
    }

    custom_app = cast_app(current_app)

    try:
        if FRONTEND_LOG_DICT is not None:
            FRONTEND_LOG_DICT[str(uuid4())] = log_entry
        else:
            custom_app.api_logger.error(
                "FRONTEND_LOG_DICT is not initialized, cannot log API call."
            )
    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to log frontend action: {str(e)}\n{traceback.format_exc}"
        )
