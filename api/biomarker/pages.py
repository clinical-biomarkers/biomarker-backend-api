"""
Defines API endpoints that provide aggregated data for initializing frontend pages.
"""

from flask_restx import Resource, Namespace

from .backend_utils import pages_utils as pages_utils

api = Namespace("pages", description="Pages API namespace.")


class HomeInit(Resource):

    @api.doc(False)
    def post(self):
        return pages_utils.home_init()

    @api.doc("home_init")
    def get(self):
        return self.post()

class Ontology(Resource):

    @api.doc(False)
    def post(self):
        return pages_utils.ontology()
    
    @api.doc("ontology")
    def get(self):
        return self.post()

api.add_resource(HomeInit, "/home_init")
api.add_resource(Ontology, "/ontology")
