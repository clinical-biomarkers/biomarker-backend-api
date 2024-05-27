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


## TODO : finish data models

### Search Simple Schema


class SearchSimple(Schema):

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


### Schema Map

SCHEMA_MAP = {"detail": DetailSchema, "search_simple": SearchSimple}
