from flask import request
from flask_restx import Namespace, Resource  # type: ignore
from .backend_utils import detail_utils as utils

api = Namespace("detail", description="Biomarker detail namespace.")


@api.doc(params={"biomarker_id": {"in": "path", "default": "AA0001-1"}})
class Detail(Resource):

    @api.doc("detail")
    def post(self, biomarker_id):
        return utils.detail(request, biomarker_id)

    def get(self, biomarker_id):
        return self.post(biomarker_id)


api.add_resource(Detail, "/<string:biomarker_id>")
