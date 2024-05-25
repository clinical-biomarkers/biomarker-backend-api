""" General purpose utility functions.
"""

from flask import Request
from typing import Dict, Union, Optional, Tuple, List, Any
import json
from marshmallow.exceptions import ValidationError
from . import db as db_utils
from .data_models import SCHEMA_MAP


def get_request_object(
    api_request: Request, endpoint: str
) -> Tuple[Optional[Dict], int]:
    """Parse the request object for the query parameters.

    Parameters
    ----------
    api_request : Request
        The flask request object.
    endpoint : str
        The endpoint

    Returns
    -------
    tuple : (dict or None, int)
        The parsed request object if availble or error object and HTTP status code.
    """
    request_object: Optional[Dict[str, Any]] = None
    if api_request.method == "GET":
        query_string = api_request.args.get("query")
        if query_string:
            # this could be avoided and can use loads function directly with marshmallow schema,
            # leaving this for now
            try:
                request_object = json.loads(query_string)
            except json.JSONDecodeError as e:
                error_obj = db_utils.log_error(
                    error_log=f"Failed to JSON decode query string.\nquery string: {query_string}\n{e}",
                    error_msg="bad-json-request",
                    origin="get_request_object",
                    sup_info="Invalid JSON formatting.",
                )
                return error_obj, 400
        else:
            request_object = {}
    elif api_request.method == "POST":
        request_object = api_request.get_json(silent=True)

    if not isinstance(request_object, dict):
        error_obj = db_utils.log_error(
            error_log=f"Decoded query/payload string JSON expected type `dict`, got `{type(request_object)}`.",
            error_msg="bad-json-request",
            origin="get_request_object",
            sup_info="Expected JSON object.",
        )
        return error_obj, 400

    if endpoint not in SCHEMA_MAP:
        error_obj = db_utils.log_error(
            error_log=f"Endpoint `{endpoint}` not found in schema map.",
            error_msg="internal-routing-error",
            origin="get_request_object",
        )
        return error_obj, 500

    schema = SCHEMA_MAP[endpoint]()
    try:
        validated_data = schema.load(request_object)
    except ValidationError as e:
        marshmallow_errors = e.messages_dict
        error_obj = db_utils.log_error(
            error_log=f"Validation error: {e.messages_dict}",
            error_msg="json-validation-error",
            origin="get_request_object",
            validation_errors=marshmallow_errors,
        )
        return error_obj, 400

    return strip_object(validated_data), 200  # type: ignore


def strip_object(target: Dict) -> Dict:
    """Strips string type dictionary keys and values of
    leading or trailing whitespace.

    Parameters
    ----------
    target : dict
        The dictionary to strip.

    Returns
    -------
    dict
        The cleaned dictionary.
    """
    target = {
        (k.strip() if isinstance(k, str) else k): (
            v.strip() if isinstance(v, str) else v
        )
        for k, v in target.items()
    }
    return target
