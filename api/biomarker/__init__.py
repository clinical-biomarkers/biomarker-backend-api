from flask_cors import CORS
from flask_restx import Api, apidoc, Resource
from flask import request, g, render_template
from pymongo import MongoClient
import os
import json
import logging
import time
from logging import Logger
from logging.handlers import RotatingFileHandler
from typing import Dict

from .backend_utils import CustomFlask, logging_status
from .backend_utils import logging_utils
from .backend_utils.performance_logger import PerformanceLogger
from .biomarker import api as biomarker_api
from .auth import api as auth_api
from .log import api as log_api
from .pages import api as pages_api

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"


class CustomApi(Api):
    def _register_specs(self, app_or_blueprint):
        pass

    @property
    def __schema__(self) -> Dict:
        # Override the __schema__ property if you need to modify the schema
        schema: Dict = super().__schema__.copy()
        if "/auth/contact" in schema["paths"] and not schema["paths"]["/auth/contact"]:
            del schema["paths"]["/auth/contact"]
        if "/log/logging" in schema["paths"] and not schema["paths"]["/log/logging"]:
            del schema["paths"]["/log/logging"]
        # schema["basePath"] = "/api"  # Set the basePath here
        return schema


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
    if logging_status is None:
        app.api_logger.info("Sqlite DB initialization successful.")
    else:
        app.api_logger.error(f"Error initializing Sqlite DB: {str(logging_status)}")

    app.performance_logger = PerformanceLogger(logger=app.api_logger)

    @app.before_request
    def start_timer():
        g.start_time = time.time()

    @app.after_request
    def log_request(response):
        duration = time.time() - g.start_time
        logging_utils.api_log(
            request_object=request.json if request.is_json else request.args.to_dict(),
            endpoint=request.path,
            api_request=request,
            duration=duration,
            status_code=response.status_code,
        )
        return response

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

    @apidoc.apidoc.add_app_template_global
    def swagger_static(filename):
        return f"./swaggerui/{filename}"

    # setup the api using the flask_restx library
    # api = Api(
    #     app,
    #     version="1.0",
    #     title="Biomarker APIs",
    #     description="Biomarker Knowledgebase API",
    # )

    api = CustomApi(
        app,
        version="1.0",
        title="Biomarker APIs",
        description="Biomarker Knowledgebase API",
    )

    @api.route("/swagger.json")
    class SwaggerJson(Resource):
        def get(self):
            swagger_spec = api.__schema__.copy()
            return swagger_spec

    @api.documentation
    def custom_ui():
        return render_template(
            "swagger-ui.html", title=api.title, specs_url="./swagger.json"
        )

    api.add_namespace(biomarker_api)
    api.add_namespace(auth_api)
    api.add_namespace(log_api)
    api.add_namespace(pages_api)

    return app
