from marshmallow import Schema, fields

# TODO @miguel
# I'm thinking the schemas are defined as constants and then those constants can be defined in a map.
# For example:
# DETAIL_SCHEMA = <schema>
# SEARCH_SIMPLE_SCHEMA = <schema>
# SCHEMA_MAP = {
#   "detail": DETAIL_SCHEMA,
#   "search_simple": SEARCH_SIMPLE_SCHEMA
# }
# This way the util file will just import SCHEMA_MAP and then in get_request_file

# TODO @miguel
# Some notes:
# Detail endpoint - from the paginated tables payload from my testing, if
# "paginated_tables" is included then the only required key in the individual
# JSON objects is "table_id". If any of "offset", "limit", "sort", or "order"
# are missing then they just get set to default values.

SCHEMA_MAP = {}
