"""
Defines API endpoints related to logging actions from the frontend.
"""

from flask import request
from flask_restx import Resource, Namespace

from .backend_utils import logging_utils

api = Namespace("log", description="Logging API namespace.")


class FrontendLogging(Resource):

    @api.doc(False)
    def post(self):
        return logging_utils.frontend_log(request)

    @api.doc(False)
    def get(self):
        return self.post()

api.add_resource(FrontendLogging, "/logging")
