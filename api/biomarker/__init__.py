from flask_cors import CORS  # type: ignore
from flask_restx import Api  # type: ignore
from pymongo import MongoClient
import os
import json
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler

from .backend_utils import CustomFlask
from .backend_utils.performance_logger import PerformanceLogger
from .biomarker import api as biomarker_api
from .auth import api as auth_api

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"


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

    app.performance_logger = PerformanceLogger(logger=app.api_logger)

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
        description="Biomarker APIs",
        openapi="3.0.2",
    )

    api.add_namespace(biomarker_api)
    api.add_namespace(auth_api)

    return app
