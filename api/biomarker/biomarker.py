from flask import request
from flask_restx import Resource, Namespace, fields
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


full_search_model = api.model(
    "Biomarker Search Query",
    {
        "biomarker_id": fields.String(required=True, default="AN6278-5"),
        "canonical_id": fields.String(required=True, default="AN6278"),
        "biomarker": fields.String(required=True, default="increased IL6 level"),
        "biomarker_entity_name": fields.String(required=True, default="Interleukin-6"),
        "biomarker_entity_id": fields.String(required=True, default="P05231-1"),
        "biomarker_entity_type": fields.String(required=True, default="protein"),
        "specimen_name": fields.String(required=True, default="blood"),
        "specimen_id": fields.String(required=True, default="0000178"),
        "specimen_loinc_code": fields.String(required=True, default="26881-3"),
        "best_biomarker_role": fields.String(required=True, default="prognostic"),
        "condition_id": fields.String(required=True, default="DOID:10283"),
        "condition_name": fields.String(required=True, default="prostate cancer"),
        "publication_id": fields.String(required=True, default="10914713"),
    },
)


class FullSearch(Resource):

    @api.doc("search")
    @api.expect(full_search_model, validate=False)
    def post(self):
        return search_utils.full_search(request)

    @api.doc(False)
    def get(self):
        return self.post()


list_model = api.model(
    "List Query",
    {"id": fields.String(required=True, default="3def43533cf6434b633cd18d1a7da5b2")},
)


class List(Resource):

    @api.doc("list")
    @api.expect(list_model, validate=False)
    def post(self):
        return list_utils.list(request)

    @api.doc(False)
    def get(self):
        return self.post()


api.add_resource(Detail, "/detail/<string:biomarker_id>")
api.add_resource(SearchInit, "/search_init")
api.add_resource(SearchSimple, "/search_simple")
api.add_resource(FullSearch, "/search")
api.add_resource(List, "/list")
