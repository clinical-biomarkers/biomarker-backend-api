from flask import Flask
from flask_cors import CORS  # type: ignore
from flask_restx import Api  # type: ignore
from pymongo import MongoClient
from pymongo.database import Database
from typing import Dict
import os
import json
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler

from .detail import api as detail_api

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"

class CustomFlask(Flask):
    hit_score_config: Dict
    mongo_db: Database
    api_logger: Logger

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


def create_app():

    # create flask instance
    app = CustomFlask(__name__)

    app.api_logger = setup_logging()
    app.api_logger.info("API Started")

    CORS(app)

    # load in config data
    api_root = os.path.realpath(os.path.dirname(__file__))
    hit_score_conf_path = os.path.join(api_root, "conf/hit_score_config.json")
    with open(hit_score_conf_path, "r") as f:
        app.hit_score_config = json.load(f)

    # initialize mongo client database handle
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[DB_NAME]
    app.mongo_db = mongo_db

    # setup the api using the flask_restx library
    api = Api(
        app,
        version="1.0",
        title="Biomarker APIs",
        description="Biomarker Knowledgebase API",
    )
    # TODO : add
    api.add_namespace(detail_api)

    return app
