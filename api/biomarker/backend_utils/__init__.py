from flask import Flask 
from pymongo.database import Database
from typing import Dict
from logging import Logger

DB_COLLECTION = "biomarker_collection"
SEARCH_CACHE_COLLECTION = "search_cache"
REQ_LOG_COLLECTION = "request_log_collection"
REQ_LOG_MAX_LEN = 20_000
ERROR_LOG_COLLECTION = "error_log_collection"
CACHE_BATCH_SIZE = 5_000
SEARCH_BATCH_SIZE = 500
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"
TIMEZONE = "US/Eastern"

class CustomFlask(Flask):
    hit_score_config: Dict
    mongo_db: Database
    api_logger: Logger
