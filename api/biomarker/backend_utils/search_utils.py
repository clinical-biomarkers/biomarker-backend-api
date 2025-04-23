"""
Handles the backend logic for the biomarker search endpoints.
"""

from flask import Request, current_app
from typing import Tuple, Dict, List

from . import db as db_utils
from . import utils
from . import SEARCH_CACHE_COLLECTION

# Mapping from API request field names to MongoDB document paths
_SEARCH_FIELD_MAP = {
    "biomarker_id": "biomarker_id",
    "canonical_id": "biomarker_canonical_id",
    "biomarker": "biomarker_component.biomarker",
    "biomarker_entity_name": "biomarker_component.assessed_biomarker_entity.recommended_name",
    "biomarker_entity_id": "biomarker_component.assessed_biomarker_entity_id",
    "biomarker_entity_type": "biomarker_component.assessed_entity_type",
    "specimen_name": "biomarker_component.specimen.name",
    "specimen_id": "biomarker_component.specimen.id",
    "specimen_loinc_code": "biomarker_component.specimen.loinc_code",
    "best_biomarker_role": "best_biomarker_role.role",
    "publication_id": "citation.reference.id",
    "condition_id": "condition.recommended_name.id",
    "condition_name": "condition.recommended_name.name",
    "condition_synonym_id": "condition.synonyms.id",
    "condition_synonym_name": "condition.synonyms.name",
}


def init() -> Tuple[Dict, int]:
    """Provides initial data for populating search interfaces.

    Retrieves available biomarker roles and assessed entity types from database
    statistics. Also includse predefined categories for simple search.

    Returns
    -------
    tuple: (dict, int)
        A dictionary containing lists of roles, types, and categories, and the
        HTTP status code.
    """
    entity_type_splits, splits_http_code = db_utils.get_stats(mode="split")
    if splits_http_code != 200:
        return entity_type_splits, splits_http_code
    try:
        entity_types = {
            (
                entity_type_obj["entity_type"].upper()
                if entity_type_obj["entity_type"] in {"dna", "rna"}
                else entity_type_obj["entity_type"]
            )
            for entity_type_obj in entity_type_splits["entity_type_splits"]
            if entity_type_obj["entity_type"]
        }
    except Exception as e:
        error_object = db_utils.log_error(
            error_log=f"Unexpected error in search init.\n{e}",
            error_msg="internal-server-error",
            origin="init",
        )
        return error_object, 500
    response_object = {
        "best_biomarker_role": [
            "prognostic",
            "diagnostic",
            "monitoring",
            "risk",
            "predictive",
            "safety",
            "response",
        ],
        "assessed_entity_type": list(entity_types),
        "simple_search_category": [
            {"id": "any", "display": "Any"},
            {"id": "biomarker", "display": "Biomarker"},
            {"id": "condition", "display": "Condition"},
        ],
    }
    return response_object, 200


def simple_search(api_request: Request) -> Tuple[Dict, int]:
    """Hanles simple search requests.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        Dictionary containing the list_id on success, or an error object,
        and the HTTP status code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "search_simple"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    mongo_query = _search_query_builder(
        request_object=request_arguments, simple_search_flag=True
    )
    # Check if a valid query was built (e.g., term_category was valid)
    if not mongo_query:
        error_obj = db_utils.log_error(
            error_log=f"Failed to build query for simple search. Request: {request_arguments}",
            error_msg="invalid-search-parameters",
            origin="simple_search",
        )
        return error_obj, 400

    return_object, query_http_code = db_utils.search_and_cache(
        request_object=request_arguments,
        query_object=mongo_query,
        search_type="simple",
        cache_collection=SEARCH_CACHE_COLLECTION,
    )

    return return_object, query_http_code


def full_search(api_request: Request) -> Tuple[Dict, int]:
    """Handles advanced (full) search requests.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        A dictionary containing the list_id on success, or an error object,
        and the HTTP status code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "search_full"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    mongo_query = _search_query_builder(
        request_object=request_arguments, simple_search_flag=False
    )

    return_object, query_http_code = db_utils.search_and_cache(
        request_object=request_arguments,
        query_object=mongo_query,
        search_type="full",
        cache_collection=SEARCH_CACHE_COLLECTION,
    )

    return return_object, query_http_code


def _search_query_builder(request_object: Dict, simple_search_flag: bool) -> Dict:
    """Builds the MongoDB query object based on search request parameters.

    Parameters
    ----------
    request_object: dict
        The validated request object containing search criteria.
    simple_search_flag: bool
        True if simple search, False for full search.

    Returns
    -------
    dict
        The MongoDB query.
    """
    query_list: List[Dict] = []
    mongo_query: Dict = {}

    if simple_search_flag:
        search_term = request_object.get("term", "")
        term_category = request_object.get("term_category", "").strip().lower()

        if not search_term:
            return mongo_query

        regex_term = utils.prepare_search_term(term=search_term, wrap=False)
        text_search_term = utils.prepare_search_term(term=search_term, wrap=False)

        if term_category == "any":
            mongo_query = {"$text": {"$search": text_search_term}}

        elif term_category == "biomarker":
            biomarker_fields = {
                k: v for k, v in _SEARCH_FIELD_MAP.items() if "condition" not in k
            }
            query_list = [
                {
                    path: {"$regex": regex_term, "$options": "i"}
                    for path in biomarker_fields.values()
                }
            ]
            mongo_query = {"$or": query_list} if query_list else {}

        elif term_category == "condition":
            condition_fields = {
                k: v for k, v in _SEARCH_FIELD_MAP.items() if "condition" in k
            }
            query_list = [
                {path: {"$regex": regex_term, "$options": "i"}}
                for path in condition_fields.values()
            ]
            mongo_query = {"$or": query_list} if query_list else {}
        else:
            custom_app = db_utils.cast_app(current_app)
            custom_app.api_logger.warning(
                f"Invalid term_category `{term_category}` encountered in _search_query_builder."
            )
            return {}

    else:  # Full search
        operation = request_object.get("operation", "and").lower().strip()
        mongo_operator = f"${operation}" if operation in ["and", "or"] else "$and"

        for key, value in request_object.items():
            if key in _SEARCH_FIELD_MAP and value:
                regex_term = utils.prepare_search_term(term=str(value), wrap=False)
                query_list.append(
                    {_SEARCH_FIELD_MAP[key]: {"$regex": regex_term, "$options": "i"}}
                )

        if query_list:
            mongo_query = {mongo_operator: query_list}
        else:
            return {}

    return mongo_query
