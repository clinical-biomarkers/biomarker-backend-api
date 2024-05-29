from flask import request
from flask_restx import Resource  # type: ignore
from .backend_utils import detail_utils as utils
from .namespaces import biomarker_ns

api = biomarker_ns

class Detail(Resource):

    @api.doc("detail")
    def post(self, biomarker_id):
        return utils.detail(request, biomarker_id)

    def get(self, biomarker_id):
        return self.post(biomarker_id)

api.add_resource(Detail, "/detail/<string:biomarker_id>")
