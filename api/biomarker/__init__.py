"""
Initializes the Biomarker Flask application and configures its components.
"""

from flask_cors import CORS
from flask_restx import Api, apidoc, Resource
from flask import request, g, render_template
from flask_jwt_extended import JWTManager
import datetime
from pymongo import MongoClient
import os
import sys
import time
from typing import Dict, List

from .backend_utils import CustomFlask, init_api_log_db, setup_logging
from .backend_utils import logging_utils
from .backend_utils.performance_logger import PerformanceLogger
from .biomarker import api as biomarker_api
from .auth import api as auth_api
from .log import api as log_api
from .pages import api as pages_api
from .data import api as data_api
from .event import api as event_api

MONGO_URI = os.getenv("MONGODB_CONNSTRING")
DB_NAME = "biomarkerdb_api"


class CustomApi(Api):
    """Custom Flask-Restx class to modify the generated Swagger schema. Serves as
    a middlware to allow manual editing of the swagger schema JSON before it is served.
    """

    def _register_specs(self, app_or_blueprint):
        pass

    @property
    def __schema__(
        self,
        endpoints_to_remove: List[str] = ["/auth/contact", "/log/logging"],
        namespaces_to_remove: List[str] = ["log", "default"],
    ) -> Dict:
        """Overrides the default schema generation:
        - Hides internal paths like /auth/contact, /log/logging, etc.
        - Removes internal namespaces ('log', 'default')
        """
        # Get the default schema
        schema: Dict = super().__schema__.copy()

        # Remove paths from swagger JSON
        for path in endpoints_to_remove:
            if path in schema["paths"] and not schema["paths"][path]:
                del schema["paths"][path]
        if "/swagger.json" in schema["paths"]:
            del schema["paths"]["/swagger.json"]

        # Remove the namespaces from the list of tags
        ns = schema["tags"]
        ns = [x for x in ns if x["name"] not in namespaces_to_remove]
        schema["tags"] = ns

        # Optionally set a base path override here
        # schema["basePath"] = "/api"

        return schema


def create_app():
    """Creates and configures the Flask application instance."""

    # Initialize the Flask app using the custom class
    app = CustomFlask(__name__)

    # --- Logging Setup ---
    app.api_logger = setup_logging()
    app.api_logger.info("API Started")

    # Initialize the SQLite database for API request logging
    api_log_db_status, api_log_db_msg = init_api_log_db()
    if api_log_db_status:
        app.api_logger.info(api_log_db_msg)
    else:
        app.api_logger.error(api_log_db_msg)
        sys.exit(1)  # Exit if logging database initilization fails

    app.performance_logger = PerformanceLogger(logger=app.api_logger)

    # --- Request Hooks ---
    @app.before_request
    def start_timer():
        """Start a timer before each request."""
        g.start_time = time.time()

    @app.after_request
    def log_request(response):
        """Log API request details after each request is processed."""
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
        """Close the SQLite database connection when the app context ends."""
        log_db = g.pop("log_db", None)
        if log_db is not None:
            log_db.close()

    # --- Extensions Initialization ---
    CORS(app)

    app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = datetime.timedelta(days=1)
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = datetime.timedelta(days=30)
    app.config["PROPAGATE_EXCEPTIONS"] = True
    jwt = JWTManager(app)

    # --- Configuration Loading ---
    # api_root = os.path.realpath(os.path.dirname(__file__))
    # hit_score_conf_path = os.path.join(api_root, "conf/hit_score_config.json")
    # with open(hit_score_conf_path, "r") as f:
    #     app.hit_score_config = json.load(f)

    # --- Database Connection ---
    # Initialize mongo client database handle
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[DB_NAME]
        app.mongo_db = mongo_db
        app.api_logger.info("MongoDB connection successful")
    except Exception as e:
        app.api_logger.error(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # --- API Setup ---
    # Serve the custom swagger.json
    @apidoc.apidoc.add_app_template_global
    def swagger_static(filename):
        return f"./swaggerui/{filename}"

    api = CustomApi(
        app,
        version="1.0",
        title="Biomarker APIs",
        description="Biomarker Knowledgebase API",
    )

    @api.route("/swagger.json")
    class SwaggerJson(Resource):
        """Serves the generated OpenAPI spec."""
        def get(self):
            swagger_spec = api.__schema__.copy()
            return swagger_spec

    @api.documentation
    def custom_ui():
        """Renders the custom Swagger UI interface."""
        return render_template(
            "swagger-ui.html", title=api.title, specs_url="./swagger.json"
        )

    # Add API namespaces
    api.add_namespace(biomarker_api)
    api.add_namespace(auth_api)
    api.add_namespace(log_api)
    api.add_namespace(pages_api)
    api.add_namespace(data_api)
    api.add_namespace(event_api)

    return app
