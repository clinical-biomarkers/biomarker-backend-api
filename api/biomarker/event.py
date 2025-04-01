from flask import request
from flask_restx import Resource, Namespace, fields
from flask_jwt_extended import jwt_required

from .backend_utils import event_utils

api = Namespace("event", description="Event API namespace.")

addnew_model = api.model(
    "Event Addnew Query",
    {
        "title": fields.String(required=True, description="Event title"),
        "description": fields.String(required=True, description="Event description"),
        "start_date": fields.String(
            required=True, description="Start date (MM/DD/YYYY HH:MM:SS)"
        ),
        "end_date": fields.String(
            required=True, description="End date (MM/DD/YYYY HH:MM:SS)"
        ),
        "venue": fields.String(required=True, description="Event venue"),
        "url": fields.String(required=True, description="Event URL"),
        "url_name": fields.String(required=True, description="Event URL name"),
        "visibility": fields.String(
            required=True, description="Event visibility (visible/hidden)"
        ),
    },
)


class EventAddnew(Resource):
    @api.expect(addnew_model)
    @jwt_required()
    def post(self):
        """Add a new event."""
        return event_utils.event_addnew(request)

    @api.doc(False)
    def get(self):
        return self.post()


detail_model = api.model(
    "Event Detail Query", {"id": fields.String(required=True, description="Event ID")}
)


class EventDetail(Resource):
    @api.expect(detail_model)
    def post(self):
        """Get event details."""
        return event_utils.event_detail(request)

    @api.doc(False)
    def get(self):
        return self.post()


list_model = api.model(
    "Event List Query",
    {
        "visibility": fields.String(
            required=True, description="Visibility filter (all/visible/hidden)"
        ),
        "status": fields.String(
            required=True, description="Status filter (all/current)"
        ),
    },
)


class EventList(Resource):
    @api.expect(list_model)
    def post(self):
        """List events with filters."""
        return event_utils.event_list(request)

    @api.doc(False)
    def get(self):
        return self.post()


update_model = api.model(
    "Event Update Query",
    {
        "id": fields.String(required=True, description="Event ID"),
        "visibility": fields.String(
            required=True, description="Event visibility (visible/hidden)"
        ),
    },
)


class EventUpdate(Resource):
    @api.expect(update_model)
    @jwt_required()
    def post(self):
        """Update an existing event."""
        return event_utils.event_update(request)

    @api.doc(False)
    def get(self):
        return self.post()


delete_model = api.model(
    "Event Delete Query", {"id": fields.String(required=True, description="Event ID")}
)


class EventDelete(Resource):
    @api.expect(delete_model)
    @jwt_required()
    def post(self):
        """Delete an event (soft delete)."""
        return event_utils.event_delete(request)

    @api.doc(False)
    def get(self):
        return self.post()


api.add_resource(EventAddnew, "/addnew/")
api.add_resource(EventDetail, "/detail/")
api.add_resource(EventList, "/list/")
api.add_resource(EventUpdate, "/update/")
api.add_resource(EventDelete, "/delete/")
