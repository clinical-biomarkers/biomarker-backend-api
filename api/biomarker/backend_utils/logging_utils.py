"""
Hanles the backend logic for logging API requests and frontend user actions.

Logs are written asynchronously to an SQLite database to minimize impact on
request times. Separate tables are used for API calls and frontend events.
"""

from logging import Logger
import sqlite3
import json
import time
import traceback
from typing import Optional, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

from flask import Request, current_app, g
from user_agents import parse

from . import utils
from . import (
    FRONTEND_CALL_LOG_TABLE,
    API_CALL_LOG_TABLE,
    LOG_DB_PATH,
    CONTACT_RECIPIENTS,
)
from .db import create_timestamp, cast_app
from .auth_utils import _send_email

# Prevents blocking the main request thread for logging I/O, might consider increasing
# max_workers in the future, however we would have to consider SQLite write contention
LOG_EXECUTOR = ThreadPoolExecutor(max_workers=1)

# --- Database Connection Handling ---


# Note: Since we moved to asynchronous logging this isn't really used, it is mainly just used to check that the
# database connection can be made and sending an error notifaction if not.
def get_api_log_db():
    """Gets the SQLite database connection for logging, stored in Flask's `g` object.

    Creates a new connection if one doesn't exist for the current request context.

    Returns
    -------
    sqlite3.Connection
        The SQLite database connection object.

    Raises
    ------
    sqlite3.Error
        Re-raises the original SQLite error if the connection cannot be established,
        after attempting to log and notify.
    """
    if "log_db" not in g:
        app = cast_app(current_app)
        try:
            g.log_db = sqlite3.connect(LOG_DB_PATH, check_same_thread=False)
        except sqlite3.Error as e:
            error_message = f"CRITICAL: Failed to connect to SQLite log database at `{LOG_DB_PATH}`. Error: {e}"
            app.api_logger.critical(error_message)

            email_subject = "CRITICAL: Biomarker API Log Database Connection Failure"
            email_body = (
                f"The Biomarker API failed to establish a connection to the SQLite logging database.\n\n"
                f"Database Path: {LOG_DB_PATH}\n"
                f"Timestamp: {create_timestamp()}\n"
                f"Error Details: {e}\n\n"
                f"Please investigate the server's file system permissions and database integrity."
            )
            email_send_error = _send_email(
                subject=email_subject, body=email_body, recipients=CONTACT_RECIPIENTS
            )
            if email_send_error:
                app.api_logger.error(
                    f"Failed to send critical logging DB connection failure email notification: {email_send_error}"
                )
            else:
                app.api_logger.info(
                    "Successfully sent email notification about log DB connection failure."
                )

    return g.log_db


# --- Asynchronous Logging Function ---


def _async_log_db(
    log_entry: Dict[str, Any], table_name: str, db_path: str, logger: Logger
):
    """Asynchronously writes a log entry to the specified SQLite table.

    Handles retries with exponential backoff for locked database scenarios.

    Parameters
    ----------
    log_entry: dict[str, Any]
        The dictionary containing the log data (column names as keys).
    table_name: str
        The name of the SQLite table to insert into (e.g., `api`, `frontend`).
    db_path: str
        The file path to the SQLite database.
    logger: Logger
        The logger instance to use for logging errors within this async function.
    """
    retries = 3
    delay = 0.5

    for attempt in range(retries):
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            columns = ", ".join(log_entry.keys())
            placeholders = ", ".join("?" * len(log_entry))

            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(log_entry.values()))
            conn.commit()
            return

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < retries - 1:
                logger.warning(
                    f"SQLite database locked on attempt {attempt + 1} for table `{table_name}`. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(
                    f"Failed to log entry to `{table_name}` after {retries} attempts. Error: {e}\n{traceback.format_exc()}\nEntry: {log_entry}"
                )
                return
        except sqlite3.Error as e:
            logger.error(
                f"SQLite Error logging entry to `{table_name}`. Error: {e}\n{traceback.format_exc()}\nEntry: {log_entry}"
            )
            return
        finally:
            if conn:
                conn.close()


# --- Endpoint Handlers ---


def frontend_log(api_request: Request) -> Tuple[Dict, int]:
    """Handles requests to the frontend logging endpoint.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return object and HTTP code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "frontend_logging"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    _log_frontend_action(request_arguments)

    return {"status": "success", "message": "Log entry received."}, 200


# --- Logging Triggers ---


def api_log(
    request_object: Optional[Dict],
    endpoint: str,
    api_request: Request,
    duration: float,
    status_code: int,
):
    """Logs details of an API request to the SQLite database asynchronously.

    This function is typically called from the `after_request` hook.

    Parameters
    ----------
    request_object: dict or None
        The parsed query string parameters associated with the API call (if applicable).
    endpoint: str
        The endpoint the request came in for.
    api_request: Request
        The flask request object.
    duration: float
        The duration of the request processing in seconds.
    status_code: int
        The HTTP statuc code of the response.
    """

    custom_app = cast_app(current_app)

    try:
        user_agent_string = api_request.headers.get("User-Agent", "")
        user_agent = parse(user_agent_string)
        is_bot = user_agent.is_bot
        timestamp = create_timestamp()

        log_entry = {
            "timestamp": timestamp,
            "date": timestamp.split(" ")[0],
            "endpoint": endpoint,
            "request": (
                json.dumps(request_object) if request_object is not None else None
            ),
            "user_agent": user_agent_string,
            "referer": api_request.headers.get("Referer"),
            "origin": api_request.headers.get("Origin"),
            "is_bot": str(is_bot),
            "ip": api_request.environ.get(
                "HTTP_X_FORWARDED_FOR", api_request.remote_addr
            ),
            "duration": round(duration, 6),
            "status_code": status_code,
        }

        LOG_EXECUTOR.submit(
            _async_log_db,
            log_entry,
            API_CALL_LOG_TABLE,
            LOG_DB_PATH,
            custom_app.api_logger,
        )

    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to prepare API log entry for endpoint `{endpoint}`: {e}\n{traceback.format_exc()}"
        )
        # TODO : should we send an email if this fails?


def _log_frontend_action(request_object: Dict):
    """Prepares and schedules a frontend action log entry for asynchronous DB insertion.

    Parameters
    ----------
    request_object: dict
        The validated request object from the user API call.
    """
    custom_app = cast_app(current_app)

    try:
        timestamp = create_timestamp()
        log_entry = {
            "call_id": request_object.get("id"),
            "timestamp": timestamp,
            "date": timestamp.split(" ")[0],
            "user": request_object.get("user"),
            "type": request_object.get("type"),
            "page": request_object.get("page"),
            "message": request_object.get("message"),
        }

        LOG_EXECUTOR.submit(
            _async_log_db,
            log_entry,
            FRONTEND_CALL_LOG_TABLE,
            LOG_DB_PATH,
            custom_app.api_logger,
        )

    except Exception as e:
        custom_app.api_logger.error(
            f"Failed to prepare frontend log entry: {e}\n{traceback.format_exc()}\nEntry Data: {request_object}"
        )
        # TODO : should we send an email if this fails?
