from flask import Flask
from pymongo.database import Database
from typing import Dict, Tuple
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
import sqlite3
import os
from dotenv import load_dotenv
from .performance_logger import PerformanceLogger

load_dotenv()

DB_COLLECTION = "biomarker_collection"
SEARCH_CACHE_COLLECTION = "search_cache"
STATS_COLLECTION = "stats_collection"
ONTOLOGY_COLLECTION = "ontology_collection"
REQ_LOG_COLLECTION = "request_log_collection"
ERROR_LOG_COLLECTION = "error_log_collection"
VERSION_COLLECTION = "version_collection"
USER_COLLECTION = "user_collection"

REQ_LOG_MAX_LEN = 20_000
CACHE_BATCH_SIZE = 5_000
SEARCH_BATCH_SIZE = 3_000
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"
TIMEZONE = "US/Eastern"
CONTACT_SOURCE = "biomarkerpartnership"
CONTACT_RECIPIENTS = [
    "daniallmasood@email.gwu.edu",
    "skim658@gwu.edu",
    "jeetvora@email.gwu.edu",
]

API_CALL_LOG_TABLE = "api"
FRONTEND_CALL_LOG_TABLE = "frontend"
LOG_DB_PATH = (
    f"{os.environ.get('DATA_PATH')}log_db/{os.environ.get('SERVER')}/api_logs.db"
)
os.makedirs(os.path.dirname(LOG_DB_PATH), exist_ok=True)

admin_list = os.getenv("ADMIN_LIST")
ADMIN_LIST = admin_list.split(",") if admin_list is not None else None
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
EMAIL_API_KEY = os.getenv("EMAIL_APP_PASSWORD")


def init_api_log_db() -> Tuple[bool, str]:
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

        # create api log table
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

        # create frontend log table
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

    except Exception as e:
        return False, f"Failed to initialize api log db: {e}"

    return True, "Successfully initialized SQLite database tables"


def setup_logging() -> Logger:
    handler = RotatingFileHandler("app.log", maxBytes=50000000, backupCount=2)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger("biomarker_api_logger")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


class CustomFlask(Flask):
    hit_score_config: Dict
    mongo_db: Database
    api_logger: Logger
    performance_logger: PerformanceLogger
