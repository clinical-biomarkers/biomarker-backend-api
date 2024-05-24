""" General purpose utility functions.
"""

from flask import Request
from typing import Dict, Union, Optional, Tuple, List
import json
from . import db as db_utils
from .data_models import SCHEMA_MAP


# TODO : @miguel the return type annotations are going to change depending on marshmallow validation
def get_request_object(api_request: Request, endpoint: str) -> Optional[Dict]:
    """Parse the request object for the query parameters.

    Parameters
    ----------
    api_request : Request
        The flask request object.
    endpoint : str
        The endpoint

    Returns
    -------
    dict or None
        The parse request object if available or None if not.
    """
    query_string = api_request.args.get("query")
    if query_string:
        try:
            request_object = json.loads(query_string)
        except json.JSONDecodeError as e:
            db_utils.log_error(
                error_msg=f"Failed to JSON decode query string.\nquery string: {query_string}\n{e}",
                origin="get_request_object",
            )
            return None
    else:
        request_object = api_request.get_json(silent=True)

    if not isinstance(request_object, dict):
        db_utils.log_error(
            error_msg=f"Decoded query string JSON expected type `dict`, got `{type(request_object)}`.",
            origin="get_request_object",
        )
        return None

    # TODO : @miguel validation based on endpoint
    if endpoint not in SCHEMA_MAP:
        return None
    # TODO : @miguel something like below, not sure the exact syntax
    # SCHEMA_MAP[endpoint].validate(request_object)

    return _strip_dict_keys(request_object)


# def create_mongo_query(endpoint: str, request_object: dict) -> Union[dict, list]:
    """Entry point for creating the MongoDB query."""
    # TODO :
    # function_map = {"detail": _detail_mongo_query_builder}
    # if endpoint not in function_map:
        # TODO : log error and handle
    # TODO : finish
    # return function_map[endpoint]



# def _detail_mongo_query_builder() -> Union[Dict, List]:
#     pass


def _strip_dict_keys(target: Dict) -> Dict:
    """Strips string type dictionary keys and value of
    leading or trailing white space.

    Parameters
    ----------
    target : dict
        The dictionary to strip.

    Returns
    -------
    dict
        The cleaned dictionary.
    """
    return {
        k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in target.items()
    }
