from flask import request
from flask_restx import Resource  # type: ignore
from .backend_utils import search_utils as utils
from .namespaces import biomarker_ns


class SearchInit(Resource):

    @biomarker_ns.doc("search_init")
    def post(self):
        return utils.init()

    def get(self):
        return self.post()


class SearchSimple(Resource):

    @biomarker_ns.doc("search_simple")
    def post(self):
        return utils.simple_search(request)

    def get(self):
        return self.post()


class FullSearch(Resource):

    @biomarker_ns.doc("search")
    def post(self):
        return utils.full_search(request)

    def get(self):
        return self.post()


biomarker_ns.add_resource(SearchInit, "/search_init")
biomarker_ns.add_resource(SearchSimple, "/search_simple")
biomarker_ns.add_resource(FullSearch, "/search")
