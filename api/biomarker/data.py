from flask import request
from flask_restx import Resource, Namespace, fields

from .backend_utils import download_utils as download_utils

api = Namespace("data", description="Data API namespace.")

detail_download_model = api.model(
    "Detail Download Query",
    {
        "id": fields.String(required=True, default=""),
        "download_type": fields.String(required=True, default="biomarker_detail"),
        "format": fields.String(required=True, default="json"),
        "compressed": fields.Boolean(required=True, default=False),
    },
)


class DetailDownload(Resource):

    @api.expect(detail_download_model, validate=False)
    def post(self):
        return download_utils.detail_download(request)


list_download_query = api.model(
    "List Download Query",
    {
        "id": fields.String(required=True, default=""),
        "download_type": fields.String(required=True, default="biomarker_list"),
        "format": fields.String(required=True, default="csv"),
        "compressed": fields.Boolean(required=True, default=False),
    },
)


# class ListDownload(Resource):
#
#     @api.expect(list_download_query, validate=False)
#     def post(self):
#         return download_utils.list_download(request)


section_download_query = api.model(
    "Section Download Query",
    {
        "id": fields.String(required=True, default=""),
        "download_type": fields.String(required=True, default="biomarker_section"),
        "section": fields.String(required=True, default="biomarker_component"),
        "format": fields.String(required=True, default="csv"),
        "compressed": fields.Boolean(required=True, default=False),
    },
)


class SectionDownload(Resource):

    @api.expect(section_download_query, validate=False)
    def post(self):
        return download_utils.section_download(request)


api.add_resource(DetailDownload, "/detail_download")
# api.add_resource(ListDownload, "/list_download")
api.add_resource(SectionDownload, "/section_download")
