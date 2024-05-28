from flask import request
from flask_restx import Namespace, Resource # type: ignore
from .backend_utils import list_utils as utils

api = Namespace("list", description="Biomarker list namespace.")

class List(Resource):

    @api.doc("list_search")
    def post(self):
        return utils.list(request)

    def get(self):
        return self.post()

api.add_resource(List, "/search")
