"""
Handles the backend logic for the biomarker detail endpoints.
"""

from flask import Request, current_app
from typing import Tuple, Dict, List

from . import db as db_utils
from . import utils

# Defines the fields available for sorting within specific sub-arrays (tables)
# in the biomarker detail response. Keys are the table_ids from the request.
_SORTABLE_FIELDS_PER_TABLE = {
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
    api_request: Request
        The flask request object.
    biomarker_id: str
        The biomarker ID provided in the URL path.

    Returns
    -------
    tuple: (dict, int)
        The biomarker data document or an error object, and the HTTP status code.
    """
    if not biomarker_id:
        # This check is redundant as Flask routing ensures this, but leaving for now
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
    # If the request arguments are invalid just skip them
    if request_http_code == 200 and "paginated_tables" in request_arguments:
        return_object = _process_document(return_object, request_arguments)

    biomarker_data = _add_metadata(return_object)
    return biomarker_data, 200


def _add_metadata(document: Dict) -> Dict:
    """Adds metadata to the biomarker document, primarily section statistics.

    Parameters
    ----------
    document: dict
        The retrieved biomarker document.

    Returns
    -------
    dict
        The document augmented with a 'section_stats' key.
    """
    section_stats: List[Dict] = []

    component_count = len(document.get("biomarker_component", []))
    biomarker_component_stats = {
        "table_id": "biomarker_component",
        "table_stats": [{"field": "total", "count": component_count}],
        "sort_fields": sorted(
            list(_SORTABLE_FIELDS_PER_TABLE.get("biomarker_component", set()))
        ),
    }
    section_stats.append(biomarker_component_stats)

    citation_count = len(document.get("citation", []))
    citation_stats = {
        "table_id": "citation",
        "table_stats": [{"field": "total", "count": citation_count}],
        "sort_fields": sorted(list(_SORTABLE_FIELDS_PER_TABLE.get("citation", set()))),
    }
    section_stats.append(citation_stats)

    document["section_stats"] = section_stats

    for cf in document.get("crossref", []):
        if "categories" in cf and not cf.get("categories", None):
            del cf["categories"]

    return document


def _process_document(document: Dict, request_object: Dict) -> Dict:
    """Sorts and paginates specified sub-arrays within the main biomarker document.

    Parameters
    ----------
    document: dict
        The retrieved MongoDB document to process.
    request_object: dict
        The request object from the user with the paginated table criteria.

    Returns
    -------
    dict
        The processed MongoDB document.
    """
    custom_app = db_utils.cast_app(current_app)

    for paginated_config in request_object.get("paginated_tables", []):

        paginated_config = utils.strip_object(paginated_config)
        table_id = paginated_config.get("table_id")
        sort_field = paginated_config.get("sort")
        sort_order = paginated_config.get("order", "desc").lower()
        limit = paginated_config.get("limit", 100)
        offset = paginated_config.get("offset", 1)
        zero_based_offset = max(0, offset - 1)

        # --- Validation ---

        if (
            not table_id
            or table_id not in document
            or not isinstance(document[table_id], list)
        ):
            custom_app.api_logger.warning(
                f"Invalid or missing table_id `{table_id}` in paginated_tables config or document. Skipping."
            )
            continue

        if table_id not in _SORTABLE_FIELDS_PER_TABLE:
            custom_app.api_logger.warning(
                f"Table ID `{table_id}` is not configured for sorting. Skipping sort."
            )
            sort_field = None

        if sort_field and sort_field not in _SORTABLE_FIELDS_PER_TABLE.get(
            table_id, set()
        ):
            custom_app.api_logger.warning(
                f"Invalid sort_field `{sort_field}` for table_id `{table_id}`. Skipping sort."
            )
            sort_field = None

        if sort_order not in ["asc", "desc"]:
            custom_app.api_logger.warning(
                f"Invalid sort_order `{sort_order}`. Defaulting to `desc`."
            )
            sort_order = "desc"

        # --- Sorting ---
        sub_array = document[table_id]
        if sort_field:
            reverse = sort_order == "desc"
            try:

                def sort_key_getter(item: Dict):
                    if (
                        table_id == "biomarker_component"
                        and sort_field == "assessed_biomarker_entity"
                    ):
                        return (
                            item.get(sort_field, {})
                            .get("recommended_name", {})
                            .get("name", "")
                        )
                    else:
                        return item.get(sort_field)

                sub_array = sorted(
                    sub_array,
                    key=lambda x: (sort_key_getter(x) is None, sort_key_getter(x)),
                    reverse=reverse,
                )
            except TypeError as e:
                custom_app.api_logger.error(
                    f"TypeError during sorting table `{table_id}` by field `{sort_field}`. Check data types. Error: {e}"
                )
            except Exception as e:
                custom_app.api_logger.error(
                    f"Unexpected error sorting table `{table_id}` by field `{sort_field}`. Error: {e}"
                )

        # --- Pagination ---
        try:
            paginated_array = sub_array[zero_based_offset : zero_based_offset + limit]
            document[table_id] = paginated_array
        except IndexError:
            custom_app.api_logger.warning(
                f"IndexError during pagination for table `{table_id}`. Offset: {offset}, Limit: {limit}. Returning available slice."
            )
            document[table_id] = sub_array[zero_based_offset:]
        except Exception as e:
            custom_app.api_logger.error(
                f"Unexpected error during pagination for table `{table_id}`. Error: {e}"
            )

    return document


def _detail_query_builder(
    request_object: Dict,
) -> Tuple[Dict[str, str], Dict[str, int]]:
    """Builds the MongoDB query and projection for the biomarker detail endpoint.

    Parameters
    ----------
    request_object: dict
        The validated request object from the user API call.

    Returns
    -------
    tuple: (dict[str, str], dict[str, int])
        The MongoDB query for the detail endpoint and the projection object.
    """
    projection_object = {"_id": 0, "all_text": 0}
    mongo_query = {"biomarker_id": request_object["biomarker_id"]}
    return mongo_query, projection_object
