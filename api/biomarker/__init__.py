from flask_cors import CORS
from flask_restx import Api, apidoc, Resource
from flask import request, g, render_template
from pymongo import MongoClient
import os
import json
import sys
import time
from typing import Dict

from .backend_utils import CustomFlask, init_api_log_db, setup_logging
from .backend_utils import logging_utils
from .backend_utils.performance_logger import PerformanceLogger
from .biomarker import api as biomarker_api
from .auth import api as auth_api
from .log import api as log_api
from .pages import api as pages_api
from .data import api as data_api

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"


class CustomApi(Api):
    def _register_specs(self, app_or_blueprint):
        pass

    @property
    def __schema__(self) -> Dict:
        # Override the __schema__ property if you need to modify the schema
        schema: Dict = super().__schema__.copy()
        for path in ["/auth/contact", "/log/logging"]:
            if path in schema["paths"] and not schema["paths"][path]:
                del schema["paths"][path]
        if "/swagger.json" in schema["paths"]:
            del schema["paths"]["/swagger.json"]
        ns_to_rm = ["auth", "log", "default"]
        ns = schema["tags"]
        ns = [x for x in ns if x["name"] not in ns_to_rm]
        schema["tags"] = ns
        # schema["basePath"] = "/api"  # Set the basePath here
        return schema


def create_app():

    # create flask instance
    app = CustomFlask(__name__)

    app.api_logger = setup_logging()
    app.api_logger.info("API Started")

    api_log_db_status, api_log_db_msg = init_api_log_db()
    if api_log_db_status:
        app.api_logger.info(api_log_db_msg)
    else:
        app.api_logger.error(api_log_db_msg)
        sys.exit(1)

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

    @app.teardown_appcontext
    def close_db(e=None):
        log_db = g.pop("log_db", None)
        if log_db is not None:
            log_db.close()

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
    api.add_namespace(data_api)

    return app
