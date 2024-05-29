from flask import request
from flask_restx import Resource # type: ignore
from .backend_utils import list_utils as utils
from .namespaces import biomarker_ns

api = biomarker_ns

class List(Resource):

    @api.doc("list")
    def post(self):
        return utils.list(request)

    def get(self):
        return self.post()

api.add_resource(List, "/list")
