"""
Defines API endpoints related to authentication, user management.
contact forms, and administrative actions like cache clearing.
"""

from flask import request
from flask_restx import Resource, Namespace, fields

from .backend_utils import auth_utils
from .backend_utils import cache_utils

api = Namespace("auth", description="Authentication API namespace.", hide=True)


class Contact(Resource):

    @api.doc(False)
    def post(self):
        return auth_utils.contact(request)

    @api.doc(False)
    def get(self):
        return self.post()


class Notification(Resource):

    @api.doc(False)
    def post(self):
        return auth_utils.contact_notification(request)


class ClearCache(Resource):

    @api.doc(False)
    def post(self):
        return cache_utils.clear_pipeline_cache(request)


register_model = api.model(
    "Auth Register Query",
    {
        "email": fields.String(
            required=True, description="User email", default="example@example.com"
        ),
        "password": fields.String(
            required=True, description="User password", default="password"
        ),
    },
)


class Register(Resource):
    @api.expect(register_model, validate=False)
    def post(self):
        return auth_utils.register(request)

    @api.doc(False)
    def get(self):
        return self.post()


login_model = api.model(
    "Auth Login Query",
    {
        "email": fields.String(
            required=True, description="User email", default="example@example.com"
        ),
        "password": fields.String(
            required=True, description="User password", default="password"
        ),
    },
)


class Login(Resource):
    @api.expect(login_model, validate=False)
    def post(self):
        return auth_utils.login(request)

    @api.doc(False)
    def get(self):
        return self.post()


class UserID(Resource):
    @api.doc(False)
    def get(self):
        return auth_utils.userid()


api.add_resource(Contact, "/contact")
api.add_resource(Notification, "/contact_notification")
api.add_resource(ClearCache, "/clear_cache")
api.add_resource(Register, "/register")
api.add_resource(Login, "/login")
api.add_resource(UserID, "/userid")
