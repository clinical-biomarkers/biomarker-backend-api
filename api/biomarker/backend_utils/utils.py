"""General purpose utility functions."""

from typing import Dict, Optional, Tuple, Any, Union, Type, cast
import json

from flask import Request
from marshmallow import EXCLUDE, Schema
from marshmallow.exceptions import ValidationError
from typing_extensions import deprecated

from . import db as db_utils
from .data_models import SCHEMA_MAP


def get_request_object(api_request: Request, endpoint: str) -> Tuple[Dict, int]:
    """Parse and validates the request object (JSON body or query parameters).

    Parameters
    ----------
    api_request: Request
        The flask request object.
    endpoint: str
        The name of the endpoint, used to look up the correct validation schema.

    Returns
    -------
    tuple: (dict, int)
        The parsed request object or error object and HTTP status code.
    """
    request_object: Optional[Dict[str, Any]] = None
    raw_request_content: Union[str, Dict, None] = None

    # Try parsing from query string first (for GET requests or POST with query params)
    query_string = api_request.args.get("query")
    if query_string:
        raw_request_content = query_string
        try:
            request_object = json.loads(query_string)
        except json.JSONDecodeError as e:
            error_obj = db_utils.log_error(
                error_log=f"Failed to JSON decode query string: {e}. Query string: `{query_string}`",
                error_msg="bad-json-request",
                origin="get_request_object",
                sup_info="Invalid JSON format in query parameter.",
            )
            return error_obj, 400
        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Unexpected error decoding query string: {e}. Query string: `{query_string}`",
                error_msg="unexpected-json-request-error",
                origin="get_request_object",
            )
            return error_obj, 500

    # If it's a POST request and we didn't get data from query string, try parsing JSON body
    if api_request.method == "POST" and request_object is None:
        # Use silen=True to avoid raising an exception for non-JSON content types
        request_object = api_request.get_json(silent=True)
        raw_request_content = request_object

        # Explicitly check if parsing failed or body was empty/non-JSON
        if (
            request_object is None
            and api_request.content_length is not None
            and api_request.content_length > 0
        ):
            content_type = api_request.content_type or ""
            if "application/json" in content_type.lower():
                error_obj = db_utils.log_error(
                    error_log="Failed to parse JSON payload in POST request. Body might be malformed.",
                    error_msg="bad-json-request",
                    origin="get_request_object",
                )
                return error_obj, 400
            else:
                _ = db_utils.log_error(
                    error_log="Failed to parse non-JSON payload in POST request, this might be intended",
                    error_msg="non-json-request",
                    origin="get_request_object",
                )
                # This might be intended in the future, for now just treating as an empty dict
                request_object = {}
                raw_request_content = {}

    if not isinstance(request_object, dict):
        error_obj = db_utils.log_error(
            error_log=f"Parsed request data expected type `dict`, got `{type(request_object)}`. Raw content: {raw_request_content}",
            error_msg="bad-json-request",
            origin="get_request_object",
            sup_info="Request data mustt be a JSON object.",
        )
        return error_obj, 400

    # Schema validation
    if endpoint not in SCHEMA_MAP:
        error_obj = db_utils.log_error(
            error_log=f"Endpoint `{endpoint}` not found in schema map.",
            error_msg="internal-server-error",
            origin="get_request_object",
        )
        return error_obj, 500

    schema_class: Type[Schema] = SCHEMA_MAP[endpoint]
    schema_instance = schema_class()

    try:
        loaded_data = schema_instance.load(request_object, unknown=EXCLUDE)
        validated_data: Dict[str, Any] = cast(Dict[str, Any], loaded_data)
    except ValidationError as e:
        marshmallow_errors = e.messages_dict
        error_obj = db_utils.log_error(
            error_log=f"Request validation failed for endpoint `{endpoint}`. Errors: {marshmallow_errors}. Raw data: {request_object}",
            error_msg="json-validation-error",
            origin="get_request_object",
            validation_errors=marshmallow_errors,
        )
        return error_obj, 400
    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Unexpected validation/loading error for endpoint `{endpoint}`. Error: {e}. Raw data: {request_object}",
            error_msg="internal-server-error",
            origin="get_request_object",
        )
        return error_obj, 500

    # if not isinstance(validated_data, dict):
    #     error_obj = db_utils.log_error(
    #         error_log=f"Validated JSON expected type `dict`, got `{type(validated_data)}`.",
    #         error_msg="bad-json-request",
    #         origin="get_request_object",
    #         sup_info="Expected JSON object.",
    #     )
    #     return error_obj, 400

    return strip_object(validated_data), 200


def strip_object(target: Dict) -> Dict:
    """Recursively strips leading/trailing whitespace from string values within a dictionary. Also strips whitespace
    from string keys.

    Parameters
    ----------
    target: dict
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
    """Cleans and preprocesses a string for use in MongoDB text search or regex matching.

    Strips whitespace, converts to lowercase, and optionally wraps in quotes for exact
    phrase matching in $text searches.

    Parameters
    ----------
    term: str
        The search term to preprocess.
    wrap: bool, optional
        Whether or not to wrap the term in double quotes.

    Returns
    -------
    str
        The preprocessed and sanitized string.
    """
    term = term.strip().lower()
    quoted_term = f'"{term}"' if wrap else term
    return quoted_term


@deprecated("Scores are pre-computed and already within the data records")
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
