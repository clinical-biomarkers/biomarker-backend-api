from flask import request
from flask_restx import Resource, Namespace
from .backend_utils import auth_utils as auth_utils

api = Namespace("auth", description="Authentication API namespace.", hide=True)

class Contact(Resource):

    @api.doc(False)
    def post(self):
        return auth_utils.contact(request)
    
    @api.doc(False)
    def get(self):
        return self.post()

api.add_resource(Contact, "/contact")
