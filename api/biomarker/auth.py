from flask import request
from flask_restx import Resource, Namespace  # type: ignore
from .backend_utils import auth_utils as auth_utils

api = Namespace("auth", description="Authentication API namespace.")

class Contact(Resource):

    @api.doc("contact")
    def post(self):
        return 
    
    def get(self):
        return self.post()

api.add_resource(Contact, "/contact")
