from marshmallow import Schema, fields, EXCLUDE, validate

### Detail Schemas


class _PaginatedTableSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    table_id = fields.String(required=True)
    offset = fields.Integer(required=False, missing=1)
    limit = fields.Integer(required=False, missing=200)
    sort = fields.String(required=False, missing="")
    order = fields.String(required=False, missing="desc")


class DetailSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    paginated_tables = fields.List(fields.Nested(_PaginatedTableSchema), required=False)


### Search Simple Schema


class SearchSimpleSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    operation = fields.Str(required=False)
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
    )


### Search Full Schema


class SearchFullSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    specimen_name = fields.Str(required=False)
    specimen_loinc_code = fields.Str(required=False)
    biomarker_entity_type = fields.Str(required=False)
    biomarker_entity_name = fields.Str(required=False)
    biomarker_id = fields.Str(required=False)
    condition_name = fields.Str(required=False)
    condition_id = fields.Str(required=False)
    publication_id = fields.Str(required=False)
    best_biomarker_role = fields.Str(required=False)
    operation = fields.Str(required=False, missing="and")


### List Schema


class _FilterSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    operator = fields.Str(required=False, missing="and")
    selected = fields.List(fields.Str(), required=True)


class ListSchema(Schema):

    class Meta(Schema.Meta):
        unknown = EXCLUDE

    id = fields.Str(required=True)
    offset = fields.Integer(required=False, missing=1)
    sort = fields.Str(required=False, missing="hit_score")
    limit = fields.Integer(required=False, missing=20)
    order = fields.Str(required=False, missing="desc")
    filters = fields.List(fields.Nested(_FilterSchema), required=False)


### Contact Schema


class ContactSchema(Schema):

    class Meta(Schema.Meta):
        uknown = EXCLUDE

    fname = fields.Str(required=True)
    lname = fields.Str(required=True)
    email = fields.Str(required=True)
    subject = fields.Str(required=True)
    message = fields.Str(required=True)


### Schema Map

SCHEMA_MAP = {
    "detail": DetailSchema,
    "search_simple": SearchSimpleSchema,
    "search_full": SearchFullSchema,
    "list": ListSchema,
    "contact": ContactSchema,
}
