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

SCHEMA_MAP = {}
