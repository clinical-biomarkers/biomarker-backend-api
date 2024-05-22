from flask import Flask
from flask_cors import CORS  # type: ignore
from flask_restx import Api  # type: ignore

from .dataset import api as dataset_api
from .id import api as id_api
from .unreviewed import api as unreviewwed_api
from .canonical_id import api as canonical_api
from pymongo import MongoClient
import os
import logging
from logging.handlers import RotatingFileHandler
from logging.logger import Logger  # type: ignore

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"
DB_COLLECTION = "biomarker_collection"
UNREVIEWED_COLLECTION = "unreviewed_collection"


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
    app = Flask(__name__)

    app.logger = setup_logging()  # type: ignore
    app.logger.info("API Started")

    CORS(app)

    # load in config data
    api_root = os.path.realpath(os.path.dirname(__file__))
    app.config["json_config"] = os.path.join(api_root, "config.json")

    # initialize mongo client
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[DB_NAME]
    app.mongo_db = mongo_db  # type: ignore
    app.config["DB_COLLECTION"] = DB_COLLECTION
    app.config["UNREVIEWED_COLLECTION"] = UNREVIEWED_COLLECTION

    # setup the api using the flask_restx library
    api = Api(
        app,
        version="1.0",
        title="Biomarker APIs",
        description="Biomarker Knowledgebase API",
    )
    # TODO : these will change
    api.add_namespace(dataset_api)
    api.add_namespace(id_api)
    api.add_namespace(unreviewwed_api)
    api.add_namespace(canonical_api)

    return app
