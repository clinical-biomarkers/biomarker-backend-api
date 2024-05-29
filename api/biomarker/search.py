from flask import request
from flask_restx import Resource  # type: ignore
from .backend_utils import search_utils as utils
from .namespaces import biomarker_ns

api = biomarker_ns

class SearchInit(Resource):

    @api.doc("search_init")
    def post(self):
        return utils.init()

    def get(self):
        return self.post()


class SearchSimple(Resource):

    @api.doc("search_simple")
    def post(self):
        return utils.simple_search(request)

    def get(self):
        return self.post()


class FullSearch(Resource):

    @api.doc("search")
    def post(self):
        return utils.full_search(request)

    def get(self):
        return self.post()

api.add_resource(SearchInit, "/search_init")
api.add_resource(SearchSimple, "/search_simple")
api.add_resource(FullSearch, "/search")
