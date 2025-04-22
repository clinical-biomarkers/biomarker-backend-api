"""
Initialization file for backend_utils module.
"""

import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
import os
import sqlite3
from typing import Tuple
import sys

from dotenv import load_dotenv
from flask import Flask, current_app
from pymongo.database import Database

from .performance_logger import PerformanceLogger
from .db import cast_app

load_dotenv()
custom_app = cast_app(current_app)

# --- Database Collection Names ---
DB_COLLECTION = "biomarker_collection"
SEARCH_CACHE_COLLECTION = "search_cache"
STATS_COLLECTION = "stats_collection"
ONTOLOGY_COLLECTION = "ontology_collection"
REQ_LOG_COLLECTION = "request_log_collection"
ERROR_LOG_COLLECTION = "error_log_collection"
VERSION_COLLECTION = "version_collection"
USER_COLLECTION = "user_collection"
EVENT_COLLECTION = "event_collection"

# --- General Constants ---
REQ_LOG_MAX_LEN = 20_000
CACHE_BATCH_SIZE = 5_000
SEARCH_BATCH_SIZE = 3_000
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"
TIMEZONE = "US/Eastern"

# --- Contact Form Configuration ---
CONTACT_SOURCE = "biomarkerpartnership"
contact_recipients = os.getenv("CONTACT_RECIPIENTS")
if contact_recipients is None:
    custom_app.api_logger.error(
        "Failed to find contact recipients list from .env file."
    )
    sys.exit(1)
CONTACT_RECIPIENTS = contact_recipients.split(",")

# --- SQLite Logging Configuration ---
API_CALL_LOG_TABLE = "api"
FRONTEND_CALL_LOG_TABLE = "frontend"
LOG_DB_PATH = (
    f"{os.environ.get('DATA_PATH')}log_db/{os.environ.get('SERVER')}/api_logs.db"
)
os.makedirs(os.path.dirname(LOG_DB_PATH), exist_ok=True)

# --- Authentication & API Keys Configuration ---
admin_list = os.getenv("ADMIN_LIST")
ADMIN_LIST = admin_list.split(",") if admin_list is not None else None
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
EMAIL_API_KEY = os.getenv("EMAIL_APP_PASSWORD")
GITHUB_ISSUES_TOKEN = os.getenv("GITHUB_ISSUES_TOKEN")
GITHUB_ISSUE_ASSIGNEE = os.getenv("GITHUB_ISSUE_ASSIGNEE")

# --- AI Search Configuration ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")

# --- Logging Setup ---


def init_api_log_db() -> Tuple[bool, str]:
    """Initializes the SQLite database and tables for API and frontend logging
    if they don't exist.

    Returns
    -------
    tuple: (success flag, message)
        The outcome and status message to log.
    """
    try:
        conn = sqlite3.connect(LOG_DB_PATH)
        cursor = conn.cursor()

        # Check if table already exists
        cursor.execute(
            """
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name IN (?, ?)
            """,
            (API_CALL_LOG_TABLE, FRONTEND_CALL_LOG_TABLE),
        )
        num_tables = cursor.fetchone()[0]
        if num_tables == 2:
            return True, "SQLite database already initialized, using existing tables"

        # Create api log table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {API_CALL_LOG_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                date TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                request TEXT,
                user_agent TEXT,
                referer TEXT,
                origin TEXT,
                is_bot TEXT,
                ip TEXT,
                duration REAL,
                status_code INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create frontend log table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FRONTEND_CALL_LOG_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id TEXT,
                timestamp TEXT NOT NULL,
                date TEXT NOT NULL,
                user TEXT,
                type TEXT,
                page TEXT,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()
        conn.close()
        return True, "Successfully initialized SQLite database tables"

    except Exception as e:
        print(f"Error initializing SQLite log DB: {e}")
        return False, f"Failed to initialize api log db: {e}"


def setup_logging() -> Logger:
    """Sets up the main application logger with a rotating file handler.

    Returns
    -------
    Logger
        The configured logger instance.
    """
    # Configure a rotating file handler, logs up to 50MB, keeps 2 backup files
    handler = RotatingFileHandler("app.log", maxBytes=50000000, backupCount=2)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger("biomarker_api_logger")
    if not logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


# --- Custom Flask Class for Type Hinting ---


class CustomFlask(Flask):
    """Custome Flask application class providing type hints for application-specific attributes."""

    mongo_db: Database
    api_logger: Logger
    performance_logger: PerformanceLogger
    # hit_score_config: Dict
