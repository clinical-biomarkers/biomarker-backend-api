""" Handles the backend logic for the biomarker detail endpoints.
"""

from flask import Request
from typing import Optional, Tuple, Dict

from . import db as db_utils
from . import utils as utils

# available sort fields for biomarker id detail endpoint
SORT_FIELDS = {
    "biomarker_component": {
        "biomarker",
        "assessed_biomarker_entity_id",
        "assessed_entity_type",
        "assessed_biomarker_entity",
    },
    "citation": {"title", "journal", "authors", "date"},
}


def detail(api_request: Request, biomarker_id: str) -> Tuple[Dict, int]:
    """Entry point for the backend logic of the detail endpoint, which
    takes a biomarker ID and returns the full JSON data model.

    Parameters
    ----------
    api_request : Request
        The flask request object.
    biomarker_id : str
        The biomarker id passed by the route.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    if not biomarker_id:
        error_obj = db_utils.log_error(
            error_log="Invalid request, no biomarker id provided.",
            error_msg="no-biomarker-id-provided",
            origin="detail",
        )
        return error_obj, 400

    request_object = {"biomarker_id": biomarker_id}
    mongo_query, projection_object = _detail_query_builder(request_object)
    return_object, query_http_code = db_utils.find_one(mongo_query, projection_object)

    if query_http_code != 200:
        return return_object, query_http_code

    request_arguments, request_http_code = utils.get_request_object(
        api_request, "detail"
    )
    # if the request arguments are invalid just skip them
    if request_http_code == 200 and "paginated_tables" in request_arguments:
        return_object = _process_document(return_object, request_arguments)

    biomarker_data = _add_metadata(return_object)
    return biomarker_data, 200


def _add_metadata(document: Dict) -> Dict:
    """Adds the section_stats metadata.

    Parameters
    ----------
    document : dict
        The retrieved MongoDB document to calculate metadata for.

    Returns
    -------
    dict
        The updated document with the metadata.
    """
    biomarker_component_stats = {
        "table_id": "biomarker_component",
        "table_stats": [
            {"field": "total", "count": len(document["biomarker_component"])}
        ],
        "sort_fields": list(SORT_FIELDS["biomarker_component"]),
    }
    citation_stats = {
        "table_id": "citation",
        "table_stats": [{"field": "total", "count": len(document["citation"])}],
        "sort_fields": list(SORT_FIELDS["citation"]),
    }
    document["section_stats"] = [biomarker_component_stats, citation_stats]
    # Remove categories key from crossref is empty list
    for cf in document.get("crossref", []):
        if not cf.get("categories", None):
            del document["crossref"]["categories"] 
    return document


def _process_document(document: Dict, request_object: Dict) -> Dict:
    """Sorts and paginates a biomarker record based
    on paginated tables input from the user.

    Parameters
    ----------
    document : dict
        The retrieved MongoDB document to process.
    request_object : dict
        The request object from the user with the paginated
        table criteria.

    Returns
    -------
    dict
        The processed MongoDB document.
    """
    for paginated_config in request_object.get("paginated_tables", []):

        paginated_config = utils.strip_object(paginated_config)
        table_id = paginated_config["table_id"]

        if table_id not in SORT_FIELDS or table_id not in document:
            continue

        # grab configs or set with defaults
        offset = int(paginated_config.get("offset", 1)) - 1
        limit = paginated_config["limit"]
        sort_field = paginated_config["sort"]
        sort_order = paginated_config["order"]
        reverse = sort_order == "desc"

        # handle sorting
        if sort_field in SORT_FIELDS[table_id]:
            if sort_field == "assessed_biomarker_entity_id":
                document[table_id] = sorted(
                    document[table_id],
                    key=lambda x: x.get(sort_field, {}).get("recommended_name"),
                    reverse=reverse,
                )
            else:
                document[table_id] = sorted(
                    document[table_id], key=lambda x: x.get(sort_field), reverse=reverse
                )

        # handle pagination
        document[table_id] = document[table_id][offset : offset + limit]

    return document


def _detail_query_builder(
    request_object: Dict,
) -> Tuple[Dict[str, str], Dict[str, int]]:
    """Biomarker detail query builder.

    Parameters
    ----------
    request_object : dict
        The validated request object from the user API call.

    Returns
    -------
    tuple : (dict[str, str], dict[str, int])
        The MongoDB query for the detail endpoint and the projection object.
    """
    projection_object = {"_id": 0, "all_text": 0}
    mongo_query = {"biomarker_id": request_object["biomarker_id"]}
    return mongo_query, projection_object
