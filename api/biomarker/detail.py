from flask import request
from flask_restx import Resource  # type: ignore
from .backend_utils import detail_utils as utils

class Detail(Resource):

    def post(self, biomarker_id):
        return utils.detail(request, biomarker_id)

    def get(self, biomarker_id):
        return self.post(biomarker_id)
