from flask import request
from flask_restx import Resource  # type: ignore
from .backend_utils import search_utils as utils


class SearchInit(Resource):

    def post(self):
        return utils.init()

    def get(self):
        return self.post()


class SearchSimple(Resource):

    def post(self):
        return utils.simple_search(request)

    def get(self):
        return self.post()


class FullSearch(Resource):

    def post(self):
        return utils.full_search(request)

    def get(self):
        return self.post()
