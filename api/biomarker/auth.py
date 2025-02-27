from flask import request
from flask_restx import Resource, Namespace
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


api.add_resource(Contact, "/contact")
api.add_resource(Notification, "/contact_notification")
api.add_resource(ClearCache, "/clear_cache")
