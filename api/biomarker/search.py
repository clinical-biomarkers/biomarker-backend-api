from flask import request
from flask_restx import Namespace, Resource  # type: ignore
from .backend_utils import search_utils as utils

api = Namespace("search", description="Biomarker search namespace.")


class SearchInit(Resource):

    @api.doc("init")
    def post(self):
        return utils.init()

    def get(self):
        return self.post()

class SearchSimple(Resource):

    @api.doc("simple")
    def post(self):
        return utils.simple()

api.add_resource(SearchInit, "/init")
