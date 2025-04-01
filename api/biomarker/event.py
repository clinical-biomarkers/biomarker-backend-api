from flask import request
from flask_restx import Resource, Namespace, fields
from flask_jwt_extended import jwt_required

from .backend_utils import event_utils

api = Namespace("event", description="Event API namespace.")

addnew_model = api.model(
    "Event Addnew Query",
    {
        "title": fields.String(
            required=True, description="Event title", default="some title"
        ),
        "description": fields.String(
            required=True, description="Event description", default="some description"
        ),
        "start_date": fields.String(
            required=True,
            description="Start date (MM/DD/YYYY HH:MM:SS)",
            default="01/20/2021 23:59:59",
        ),
        "end_date": fields.String(
            required=True,
            description="End date (MM/DD/YYYY HH:MM:SS)",
            default="01/20/2021 07:00:00",
        ),
        "venue": fields.String(
            required=False, description="Event venue", default="some venue"
        ),
        "url": fields.String(
            required=False, description="Event URL", default="some url"
        ),
        "url_name": fields.String(
            required=False, description="Event URL name", default="some url name"
        ),
        "visibility": fields.String(
            required=False,
            description="Event visibility (visible/hidden)",
            default="visible",
        ),
    },
)


class EventAddnew(Resource):
    @api.expect(addnew_model)
    @jwt_required()
    def post(self):
        return event_utils.event_addnew(request)

    @api.doc(False)
    def get(self):
        return self.post()


detail_model = api.model(
    "Event Detail Query",
    {
        "id": fields.String(
            required=True, description="Event ID", default="67ec3e1f1333bc73cd602fbe"
        )
    },
)


class EventDetail(Resource):
    @api.expect(detail_model)
    def post(self):
        return event_utils.event_detail(request)

    @api.doc(False)
    def get(self):
        return self.post()


list_model = api.model(
    "Event List Query",
    {
        "visibility": fields.String(
            required=True,
            description="Visibility filter (all/visible/hidden)",
            default="all",
        ),
        "status": fields.String(
            required=True, description="Status filter (all/current)", default="all"
        ),
    },
)


class EventList(Resource):
    @api.expect(list_model)
    def post(self):
        return event_utils.event_list(request)

    @api.doc(False)
    def get(self):
        return self.post()


update_model = api.model(
    "Event Update Query",
    {
        "id": fields.String(
            required=True, description="Event ID", default="67ec3e1f1333bc73cd602fbe"
        ),
        "visibility": fields.String(
            required=True,
            description="Event visibility (visible/hidden)",
            default="visible",
        ),
    },
)


class EventUpdate(Resource):
    @api.expect(update_model)
    @jwt_required()
    def post(self):
        return event_utils.event_update(request)

    @api.doc(False)
    def get(self):
        return self.post()


delete_model = api.model(
    "Event Delete Query",
    {
        "id": fields.String(
            required=True, description="Event ID", default="67ec3e1f1333bc73cd602fbe"
        )
    },
)


class EventDelete(Resource):
    @api.expect(delete_model)
    @jwt_required()
    def post(self):
        return event_utils.event_delete(request)

    @api.doc(False)
    def get(self):
        return self.post()


api.add_resource(EventAddnew, "/addnew/")
api.add_resource(EventDetail, "/detail/")
api.add_resource(EventList, "/list/")
api.add_resource(EventUpdate, "/update/")
api.add_resource(EventDelete, "/delete/")
