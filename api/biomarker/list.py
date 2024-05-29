from flask import request
from flask_restx import Resource # type: ignore
from .backend_utils import list_utils as utils

class List(Resource):

    def post(self):
        return utils.list(request)

    def get(self):
        return self.post()
