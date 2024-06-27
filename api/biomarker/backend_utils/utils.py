""" General purpose utility functions.
"""

from flask import Request, current_app
from typing import Dict, Optional, Tuple, Any
import json
from marshmallow.exceptions import ValidationError
from . import db as db_utils
from .data_models import SCHEMA_MAP


def get_request_object(api_request: Request, endpoint: str) -> Tuple[Dict, int]:
    """Parse the request object for the query parameters.

    Parameters
    ----------
    api_request : Request
        The flask request object.
    endpoint : str
        The endpoint

    Returns
    -------
    tuple : (dict, int)
        The parsed request object or error object and HTTP status code.
    """
    request_object: Optional[Dict[str, Any]] = None
    query_string = api_request.args.get("query")
    if query_string:
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
        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Unexpected error while decoding query string.\nquery string: {query_string}\n{e}",
                error_msg="unexpected-json-request-error",
                origin="get_request_object",
            )
            return error_obj, 500

    if api_request.method == "POST" and not request_object:
        request_object = api_request.get_json(silent=True)
        if request_object is None:
            error_obj = db_utils.log_error(
                error_log="Failed to parse JSON payload in POST request.",
                error_msg="bad-json-request",
                origin="get_request_object",
            )
            return error_obj, 400

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

    if not isinstance(validated_data, dict):
        error_obj = db_utils.log_error(
            error_log=f"Validated JSON expected type `dict`, got `{type(validated_data)}`.",
            error_msg="bad-json-request",
            origin="get_request_object",
            sup_info="Expected JSON object.",
        )
        return error_obj, 400

    return strip_object(validated_data), 200


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


def prepare_search_term(term: str, wrap: bool = True) -> str:
    """Cleans and preprocesses a string for use in a MongoDB search.

    Parameters
    ----------
    term : str
        The term to preprocess.
    wrap : bool (default: True)
        Whether or not to wrap the term in quotes.

    Returns
    -------
    str
        The preprocessed and sanitized string.
    """
    term = term.strip().lower()
    quoted_term = f'"{term}"' if wrap else term
    return quoted_term


def get_hit_score(doc: Dict) -> Tuple[float, Dict]:
    """calculates a hit score for a record.

    Parameters
    ----------
    doc : dict
        The document to calculate the hit score for.

    Returns
    -------
    tuple : (float, dict)
        The hit score and the score info object.
    """
    # TODO : implement hit score, hardcoding for now
    score_info = {
        "contributions": [{"c": "biomarker_exact_match", "w": 0.0, "f": 0.0}],
        "formula": "sum(w + 0.01*f)",
        "variables": {
            "c": "condition name",
            "w": "condition weight",
            "f": "condition match frequency",
        },
    }
    return 0.1, score_info
