"""
Defines Marshmallow schemas for validating API request and response data structures.
"""

from marshmallow import Schema, fields, EXCLUDE, validate
from . import constants

# --- Base Schemas ---


class _FilterSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    operator = fields.Str(required=False, missing="and")
    selected = fields.List(fields.Str(), required=True)


class _PaginatedTableSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    table_id = fields.String(required=True)
    offset = fields.Integer(required=False, missing=1)
    limit = fields.Integer(required=False, missing=200)
    sort = fields.String(required=False, missing="")
    order = fields.String(required=False, missing="desc")


# --- Detail Schemas ---


class DetailSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    paginated_tables = fields.List(
        fields.Nested(_PaginatedTableSchema),
        required=False,
        description="Optional list of table pagination/sorting configurations.",
    )


# --- Search Schemas ---


class SearchSimpleSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    operation = fields.Str(
        required=False,
        missing="AND",
        validate=validate.OneOf(["and", "AND", "or", "OR"]),
    )
    query_type = fields.Str(required=False)
    term = fields.Str(required=True)
    term_category = fields.Str(
        required=True,
        validate=validate.OneOf(
            {
                "Any",
                "any",
                "ANY",
                "Biomarker",
                "biomarker",
                "BIOMARKER",
                "Condition",
                "condition",
                "CONDITION",
            }
        ),
        description="Simple search category",
    )


class SearchFullSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    biomarker_id = fields.Str(required=False)
    canonical_id = fields.Str(required=False)
    biomarker = fields.Str(required=False)
    biomarker_entity_name = fields.Str(required=False)
    biomarker_entity_id = fields.Str(required=False)
    biomarker_entity_type = fields.Str(required=False)
    specimen_name = fields.Str(required=False)
    specimen_id = fields.Str(required=False)
    specimen_loinc_code = fields.Str(required=False)
    best_biomarker_role = fields.Str(
        required=False,
        validate=validate.OneOf(
            {
                "diagnostic",
                "Diagnostic",
                "prognostic",
                "Prognostic",
                "monitoring",
                "Monitoring",
                "risk",
                "Risk",
                "predictive",
                "Predictive",
                "safety",
                "Safety",
                "response",
                "Response",
            }
        ),
    )
    data_source = fields.Str(
        required=False,
        validate=validate.OneOf(constants.DATA_SOURCES)
    )
    condition_id = fields.Str(required=False)
    condition_name = fields.Str(required=False)
    publication_id = fields.Str(required=False)
    operation = fields.Str(
        required=False,
        missing="AND",
        validate=validate.OneOf(["and", "AND", "or", "OR"]),
    )


class AISearchSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    query = fields.Str(required=True)


# --- List Schemas ---


class ListSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    offset = fields.Integer(required=False, missing=1)
    sort = fields.Str(required=False, missing="hit_score")
    limit = fields.Integer(required=False, missing=20)
    order = fields.Str(
        required=False,
        missing="desc",
        validate=validate.OneOf(["asc", "ASC", "desc", "DESC"]),
    )
    filters = fields.List(fields.Nested(_FilterSchema), required=False)


# --- Notification Schemas ---


class ContactSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    fname = fields.Str(required=True)
    lname = fields.Str(required=True)
    email = fields.Str(required=True)
    subject = fields.Str(required=True)
    message = fields.Str(required=True)
    page = fields.Str(required=False)


class ContactNotificationSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    email = fields.List(fields.Str(), required=True)
    subject = fields.Str(required=True)
    message = fields.Str(required=True)
    api_key = fields.Str(required=True)


# --- Logging Schemas ---


class FrontendLogger(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    type = fields.Str(required=True)
    page = fields.Str(required=True)
    user = fields.Str(required=True)
    id = fields.Str(required=True)
    message = fields.Str(required=True)


# --- Clear Cache Schema ---


class ClearCacheSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    api_key = fields.Str(required=True)


# --- Download Schema ---


class DownloadSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    download_type = fields.Str(required=True)
    format = fields.Str(required=True, validate=validate.OneOf(["json", "csv"]))
    compressed = fields.Bool(required=True)
    section = fields.Str(required=False)
    filters = fields.List(fields.Nested(_FilterSchema), required=False)


# --- Login Schemas ---


class AuthLoginSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    email = fields.Str(required=True)
    password = fields.Str(required=True)


class AuthRegisterSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    email = fields.Str(required=True)
    password = fields.Str(required=True)


# --- Event Schemas ---


class EventAddNewSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    title = fields.Str(required=True)
    description = fields.Str(required=True)
    start_date = fields.Str(required=True)
    end_date = fields.Str(required=True)
    venue = fields.Str(required=False)
    url = fields.Str(required=False)
    url_name = fields.Str(required=False)
    visibility = fields.Str(
        required=False,
        missing="visible",
        validate=validate.OneOf(["visible", "hidden"]),
    )


class EventDetailSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)


class EventListSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    visibility = fields.Str(
        required=True, validate=validate.OneOf(["all", "visible", "hidden"])
    )
    status = fields.Str(required=True, validate=validate.OneOf(["all", "current"]))


class EventUpdateSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    visibility = fields.Str(
        required=True, validate=validate.OneOf(["visible", "hidden"])
    )


class EventDeleteSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)


# --- Schema Mapping ---

SCHEMA_MAP = {
    "detail": DetailSchema,
    "search_simple": SearchSimpleSchema,
    "search_full": SearchFullSchema,
    "list": ListSchema,
    "contact": ContactSchema,
    "frontend_logging": FrontendLogger,
    "notification": ContactNotificationSchema,
    "clear_cache": ClearCacheSchema,
    "download": DownloadSchema,
    "ai_search": AISearchSchema,
    "auth_login": AuthLoginSchema,
    "auth_register": AuthRegisterSchema,
    "event_addnew": EventAddNewSchema,
    "event_detail": EventDetailSchema,
    "event_list": EventListSchema,
    "event_update": EventUpdateSchema,
    "event_delete": EventDeleteSchema,
}
