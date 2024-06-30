from flask import request
from flask_restx import Resource, Namespace, fields  # type: ignore
from .backend_utils import detail_utils as detail_utils
from .backend_utils import list_utils as list_utils
from .backend_utils import search_utils as search_utils

api = Namespace("biomarker", description="Biomarker API namespace.")


class Detail(Resource):

    @api.doc(False)
    def post(self, biomarker_id):
        return detail_utils.detail(request, biomarker_id)

    @api.doc("detail")
    def get(self, biomarker_id):
        return self.post(biomarker_id)


class SearchInit(Resource):

    @api.doc(False)
    def post(self):
        return search_utils.init()

    @api.doc("search_init")
    def get(self):
        return self.post()


search_simple_model = api.model(
    "Biomarker Simple Search Query",
    {
        "term_category": fields.String(required=True, default="biomarker"),
        "term": fields.String(required=True, default="AA4686-1"),
    },
)


class SearchSimple(Resource):

    @api.doc("search_simple")
    @api.expect(search_simple_model, validate=False)
    def post(self):
        return search_utils.simple_search(request)

    @api.doc(False)
    def get(self):
        return self.post()


class FullSearch(Resource):

    @api.doc("search")
    def post(self):
        return search_utils.full_search(request)

    def get(self):
        return self.post()


class List(Resource):

    @api.doc("list")
    def post(self):
        return list_utils.list(request)

    def get(self):
        return self.post()


api.add_resource(Detail, "/detail/<string:biomarker_id>")
api.add_resource(SearchInit, "/search_init")
api.add_resource(SearchSimple, "/search_simple")
api.add_resource(FullSearch, "/search")
api.add_resource(List, "/list")
