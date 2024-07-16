from flask import Flask
from pymongo.database import Database
from typing import Optional, Dict
from logging import Logger
import os
from sqlitedict import SqliteDict  # type: ignore
from .performance_logger import PerformanceLogger

DB_COLLECTION = "biomarker_collection"
SEARCH_CACHE_COLLECTION = "search_cache"
REQ_LOG_COLLECTION = "request_log_collection"
REQ_LOG_MAX_LEN = 20_000
ERROR_LOG_COLLECTION = "error_log_collection"
CACHE_BATCH_SIZE = 5_000
SEARCH_BATCH_SIZE = 3_000
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"
TIMEZONE = "US/Eastern"
CONTACT_SOURCE = "biomarkerpartnership"
CONTACT_RECIPIENTS = ["daniallmasood@email.gwu.edu", "skim658@gwu.edu"]

API_CALL_LOG_TABLE = "api_calls"
FRONTEND_CALL_LOG_TABLE = "frontend_logs"
LOG_DB_PATH = f"{os.environ.get('DATA_PATH')}log_db/{os.environ.get('SERVER')}/api_logs.db"
os.makedirs(os.path.dirname(LOG_DB_PATH), exist_ok=True)
logging_status: Optional[Exception] = None
try:
    API_LOG_DICT: Optional[SqliteDict] = SqliteDict(
        LOG_DB_PATH, tablename=API_CALL_LOG_TABLE, autocommit=True
    )
    FRONTEND_LOG_DICT: Optional[SqliteDict] = SqliteDict(
        LOG_DB_PATH, tablename=FRONTEND_CALL_LOG_TABLE, autocommit=True
    )
except Exception as e:
    API_LOG_DICT = None
    FRONTEND_LOG_DICT = None
    logging_status = e


class CustomFlask(Flask):
    hit_score_config: Dict
    mongo_db: Database
    api_logger: Logger
    performance_logger: PerformanceLogger
